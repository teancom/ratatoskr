//! Imperative Shell — owns upstream MCP client connections on behalf of the
//! gateway.

pub mod connect;
pub mod container_runtime;
pub mod container_runtime_probe;
pub mod credential_store;
pub mod event_bus;
pub mod events;
pub mod health;
pub mod reauth_notify;
pub mod resource_list_cache;
pub mod resource_uri;
pub mod sampling;
pub mod supervision;
pub mod supervisor;
pub mod template_matcher;
pub mod tool_list_cache;
pub mod upstream;
pub(crate) mod upstream_handler;

pub use connect::{LiveUpstream, connect, connect_with_transport};
pub use credential_store::{FileCredentialStore, STATE_DIR_ENV, state_dir, state_dir_with_env};
pub use event_bus::UpstreamEventBus;
pub use events::UpstreamEvent;
pub use health::{FailureKind, HealthSnapshot, HealthState, UpstreamHealth};
pub use resource_list_cache::{ResourceListCache, ResourceListing, spawn_resource_invalidator};
pub use sampling::{PeerId, SamplingBroker, SharedSamplingBroker};
pub use supervision::{PartialSupervisionSettings, SupervisionError, SupervisionSettings};
pub use supervisor::{Connector, RealConnector, UpstreamSupervisor};
pub use tool_list_cache::{ToolListCache, spawn_invalidator};
pub use upstream::{
    DEFAULT_CALL_TIMEOUT, ReplaceError, UpstreamConfig, UpstreamError, UpstreamHandle,
    UpstreamKind, UpstreamManager, is_auth_failure,
};
