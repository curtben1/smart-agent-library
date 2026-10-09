import { createHash, createPrivateKey, createPublicKey, KeyObject } from "crypto";
import { readFileSync } from "fs";
import { v4 as uuidv4 } from "uuid";
import { sign } from "verifiable-credential-toolkit";

export interface SigningIdentity {
    did: string;
    publicKey: string;
    privateKeySeed: Uint8Array;
}

export interface PublicSigningIdentity {
    did: string;
    publicKey: string;
}

interface VoltConfigCredentialFields {
    p?: string;
    credential?: { identity_did?: string; key?: string };
}

/**
 * Reads the Volt identity DID and its Ed25519 key from a Volt configuration file. Throws when the
 * file has no identity, or its key is not an Ed25519 key.
 */
export function signingIdentityFromVoltConfig(voltConfigPath: string): SigningIdentity {
    const voltConfig = JSON.parse(readFileSync(voltConfigPath, "utf8")) as VoltConfigCredentialFields;
    const did = voltConfig.credential?.identity_did;
    const privateKeyPem = voltConfig.credential?.key;
    if (!did || !privateKeyPem) {
        throw new Error(`${voltConfigPath} has no credential.identity_did and credential.key to sign with`);
    }

    const privateKey = createPrivateKey({ key: privateKeyPem, passphrase: voltConfig.p });
    if (privateKey.asymmetricKeyType !== "ed25519") {
        throw new Error(`the credential key in ${voltConfigPath} is ${privateKey.asymmetricKeyType}, not ed25519`);
    }
    return {
        did,
        publicKey: encodedRawPublicKeyOf(createPublicKey(privateKey)),
        privateKeySeed: new Uint8Array(Buffer.from(privateKey.export({ format: "jwk" }).d as string, "base64url"))
    };
}

/** The DID and base64 Ed25519 public key of the identity in a Volt configuration file. */
export function readPublicSigningIdentity(voltConfigPath: string): PublicSigningIdentity {
    const { did, publicKey } = signingIdentityFromVoltConfig(voltConfigPath);
    return { did, publicKey };
}

function encodedRawPublicKeyOf(publicKey: KeyObject): string {
    return Buffer.from(publicKey.export({ format: "jwk" }).x as string, "base64url").toString("base64");
}

/** The trustedIssuers map key that holds the trust grant for an issuer DID. */
export function trustGrantKeyOf(issuerDid: string): string {
    return createHash("sha256").update(issuerDid).digest("hex");
}

/**
 * Builds a credential of the given types around a subject, with the identity as its issuer, and
 * signs it with the identity's key. Optional fields such as validUntil go onto the credential.
 */
export function signCredentialAs<SignedCredential>(
    identity: SigningIdentity,
    types: string[],
    credentialSubject: object,
    optionalFields: Record<string, unknown> = {}
): SignedCredential {
    const unsignedCredential = {
        "@context": ["https://www.w3.org/ns/credentials/v2"],
        id: `urn:uuid:${uuidv4()}`,
        type: ["VerifiableCredential", ...types],
        issuer: identity.did,
        validFrom: new Date().toISOString(),
        credentialSubject,
        ...optionalFields
    };
    return plainObjectOf(sign(unsignedCredential, identity.privateKeySeed)) as SignedCredential;
}

function plainObjectOf(value: unknown): unknown {
    if (value instanceof Map) {
        return Object.fromEntries(Array.from(value.entries(), ([key, entry]) => [key, plainObjectOf(entry)]));
    }
    if (Array.isArray(value)) return value.map(plainObjectOf);
    return value;
}
