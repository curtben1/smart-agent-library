import { readFileSync } from "node:fs";
import grpc from "@grpc/grpc-js";
// @ts-expect-error - Using JS module without types
import { SyncProvider, VoltClient } from "@tdxvolt/volt-client-grpc";
import * as Y from "yjs";
import { CONTROL_PLANE_DOCUMENT_GUID, readActiveVolt, readAdvertisedClientUrls } from "./controlPlaneGateway.js";
import { waitWithDeadline, type WaitReporter } from "./deadline.js";

const VOLT_DEADLINE_SECONDS = 15;

export interface VoltCandidate {
    configurationPath: string;
    voltDid: string;
}

export function readVoltCandidate(configurationPath: string): VoltCandidate {
    const configuration = JSON.parse(readFileSync(configurationPath, "utf8")) as { volt?: { id?: unknown } };
    const voltDid = configuration.volt?.id;
    if (typeof voltDid !== "string") throw new Error(`the volt configuration ${configurationPath} names no volt id`);
    return { configurationPath, voltDid };
}

function waitForSync(provider: SyncProvider): Promise<void> {
    return new Promise(resolve => {
        provider.on("synced", (isSynced: boolean) => {
            if (isSynced) resolve();
        });
    });
}

async function openStandaloneVoltClient(configurationPath: string): Promise<VoltClient> {
    const voltClient = new VoltClient(grpc);
    await voltClient.initialise(configurationPath);
    return voltClient;
}

/**
 * Proves that the volt answers before a sync provider touches it, because a provider on a volt that is
 * down throws outside any promise and ends the process.
 */
async function requireAnsweringVolt(
    voltClient: VoltClient,
    candidate: VoltCandidate,
    synapseId: string,
    reportWait?: WaitReporter
): Promise<void> {
    const response = await waitWithDeadline<{ resource?: unknown } | undefined>(
        voltClient.GetResource({ resource_id: synapseId }),
        VOLT_DEADLINE_SECONDS,
        `the volt ${candidate.voltDid} to answer for ${synapseId}`,
        reportWait
    );
    if (!response?.resource) throw new Error(`the volt ${candidate.voltDid} holds no ${synapseId}`);
}

async function readAdvertisedClientUrlsFrom(
    candidate: VoltCandidate,
    synapseId: string,
    reportWait?: WaitReporter
): Promise<string[]> {
    const voltClient: VoltClient = await waitWithDeadline(
        openStandaloneVoltClient(candidate.configurationPath),
        VOLT_DEADLINE_SECONDS,
        `the volt ${candidate.voltDid} to connect`,
        reportWait
    );
    try {
        await requireAnsweringVolt(voltClient, candidate, synapseId, reportWait);
    } catch (failure) {
        voltClient.disconnect();
        throw failure;
    }
    const document = new Y.Doc({ guid: CONTROL_PLANE_DOCUMENT_GUID });
    const provider = new SyncProvider(Y, voltClient, synapseId, document);
    try {
        const synced = waitForSync(provider);
        provider.startSync();
        await waitWithDeadline(synced, VOLT_DEADLINE_SECONDS, `the control-plane document on ${candidate.voltDid}`, reportWait);
        return readAdvertisedClientUrls(document);
    } finally {
        provider.stopSync?.();
        document.destroy();
        voltClient.disconnect();
    }
}

/**
 * The client addresses of the control-plane members, from the advertisements on the first volt that
 * answers. The volt client that the library publishes through is left as it was.
 */
export async function readAdvertisedClientUrlsFromAnyVolt(
    candidates: VoltCandidate[],
    synapseId: string,
    reportWait?: WaitReporter
): Promise<string[]> {
    const failures: string[] = [];
    for (const candidate of candidates) {
        try {
            return await readAdvertisedClientUrlsFrom(candidate, synapseId, reportWait);
        } catch (failure) {
            failures.push(`${candidate.voltDid}: ${(failure as Error).message}`);
        }
    }
    throw new Error(`no volt of the pair answered: ${failures.join("; ")}`);
}

/** The configuration, out of the given candidates, that connects to the given volt. */
export function findConfigurationOfVolt(candidates: VoltCandidate[], voltDid: string): string {
    const matchingCandidate = candidates.find(candidate => candidate.voltDid === voltDid);
    if (!matchingCandidate) {
        throw new Error(`the active volt ${voltDid} is neither the main nor the peer volt configuration`);
    }
    return matchingCandidate.configurationPath;
}

/**
 * The configuration of the volt that the hosts work against: the volt that the control plane names
 * active, out of the main and the peer configuration. Returns the main configuration when no peer
 * is given, and when the control plane names no active volt yet. Throws when the control plane names
 * a volt that neither configuration connects to.
 */
export async function selectActiveVoltConfiguration(
    mainConfigurationPath: string,
    peerConfigurationPath: string | undefined,
    synapseId: string,
    reportWait?: WaitReporter
): Promise<string> {
    if (!peerConfigurationPath) return mainConfigurationPath;
    const candidates = [mainConfigurationPath, peerConfigurationPath].map(readVoltCandidate);
    const activeVolt = await readActiveVolt(await readAdvertisedClientUrlsFromAnyVolt(candidates, synapseId, reportWait));
    if (activeVolt === null) return mainConfigurationPath;
    return findConfigurationOfVolt(candidates, activeVolt.voltDid);
}
