import type * as grpc from "@grpc/grpc-js";

/** Common status structure for all Volt API responses */
export interface Status {
  code: number;
  message: string;
  description?: string;
}

/** Attribute value types for Resource attributes */
export type AttributeValue =
  | { string: string }
  | { integer: number }
  | { real: number }
  | { boolean: boolean }
  | { bytes: Buffer };

/** Supported attribute data types */
export enum AttributeDataType {
  UNKNOWN = 0,
  STRING = 1,
  INTEGER = 2,
  REAL = 3,
  BOOLEAN = 4,
  BYTES = 5,
}

/** Resource attribute */
export interface ResourceAttribute {
  id?: number;
  attribute_id: string;
  resource_id?: string;
  data_type: AttributeDataType;
  value?: AttributeValue[];
}

/** Version metadata */
export interface Version {
  version_major: number;
  version_minor: number;
  version_patch: number;
}

/** Service method description */
export interface MethodDescription {
  path: string;
  client_streaming: boolean;
  server_streaming: boolean;
}

/** Proto file description */
export interface ProtoFile {
  file_path: string;
  protobuf: string;
  service_name?: string[];
}

/** Service description for resources exposing gRPC services */
export interface ServiceDescription {
  host_type?: string;
  host_client_id?: string;
  host_service_id?: string;
  host_address?: string;
  host_ca_pem?: string;
  host_public_key?: string;
  host_connection_id?: string;
  host_session_id?: string;
  discoverable?: string;
  ping_timestamp?: number;
  proto_file?: ProtoFile[];
  service_api?: string[];
  method?: MethodDescription[];
}

/** Online status of the resource */
export enum OnlineStatus {
  UNKNOWN = 0,
  ONLINE = 1,
  OFFLINE = 2,
}

/** Core resource metadata */
export interface Resource {
  id: string;
  description?: string;
  name: string;
  share_mode?: string;
  volt_id?: string;
  service_description?: ServiceDescription;
  attribute?: ResourceAttribute[];
  platform_version?: Version;
  version?: number;
  owner?: string;
  created?: number;
  modified?: number;
  status?: string;
  kind?: string[];
  online_status?: OnlineStatus;
  size?: number;
  store?: string;
  alias?: string[];
  content_hash?: string;
}

/** Request type for saving a resource */
export interface SaveResourceRequest {
  resource: Resource;
  create?: boolean;
  create_in_parent_id?: string;
  purge_attributes?: boolean;
}

/** Response type for saving a resource */
export interface SaveResourceResponse {
  status: Status;
  resource?: Resource;
}

/** Request type for deleting a resource */
export interface DeleteResourceRequest {
  resource_id: string;
  recursive: boolean;
}

/** Request type for publishing wire data */
export interface PublishWireRequest {
  wire_id: string;
  chunk: Buffer;
}

/** Writable wire stream */
export interface PublishWireStream {
  on(event: "end", listener: () => void): this;
  on(event: "error", listener: (err: Error) => void): this;
  end(): void;
}

/** Volt client interface wrapping API calls */
export interface VoltClient {
  initialise(configPath: string): Promise<void>;

  SaveResource(request: SaveResourceRequest): Promise<SaveResourceResponse>;

  DeleteResource(request: DeleteResourceRequest): Promise<void>;

  PublishWire(request: PublishWireRequest): PublishWireStream;
}

/** Constructor for VoltClient with injected grpc instance */
export interface VoltClientConstructor {
  new (grpcInstance: typeof grpc): VoltClient;
}

/** Request type for publishing wire data */
export interface PublishWireRequest {
  wire_id: string;
  chunk: Buffer;
  do_not_persist?: boolean;
}

/** Response type for publishing wire data */
export interface PublishWireResponse {
  status: Status;
}

/** Request type for subscribing to a wire */
export interface SubscribeWireRequest {
  wire_id?: string;
  stop?: boolean;
}

/** Response type for subscribing to a wire */
export interface SubscribeWireResponse {
  status?: Status;
  chunk?: Buffer;
}
