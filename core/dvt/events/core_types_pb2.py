# preserving import path during dbtlabs.proto refactor
from dbtlabs.proto.public.v1.fields.core_types_pb2 import *  # noqa

# ---------------------------------------------------------------------------
# DVT Federation protobuf message stubs
#
# The upstream dbt-core event system requires every event class to have a
# corresponding protobuf message class in PROTO_TYPES_MODULE (this module):
#   - A *data* message (e.g. FederationPathResolved) used by BaseEvent.__init__
#   - A *wrapper* message (e.g. FederationPathResolvedMsg) used by msg_from_base_event
#     with fields: info (CoreEventInfo) + data (the data message)
#
# Since Federation events are DVT-specific and not in the upstream .proto
# files, we dynamically generate both kinds using the protobuf descriptor API.
# ---------------------------------------------------------------------------
from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

_pool = _descriptor_pool.Default()
_sym_db = _symbol_database.Default()

# Field type shortcuts
_TYPE_STRING = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING
_TYPE_INT32 = _descriptor_pb2.FieldDescriptorProto.TYPE_INT32
_TYPE_FLOAT = _descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT
_TYPE_MESSAGE = _descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
_LABEL_OPTIONAL = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_LABEL_REPEATED = _descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED

# Fully-qualified name of the upstream CoreEventInfo message (used in *Msg wrappers)
_CORE_EVENT_INFO_FQN = "v1.public.fields.core_types.CoreEventInfo"

# Define all Federation data message types: { ClassName: [(field_name, type, label), ...] }
_FEDERATION_MESSAGES = {
    "FederationPathResolved": [
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("execution_path", _TYPE_STRING, _LABEL_OPTIONAL),
        ("target", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationSparkInitialized": [
        ("num_models", _TYPE_INT32, _LABEL_OPTIONAL),
    ],
    "FederationSparkShutdown": [],
    "FederationSourceExtractStart": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("table_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("target", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationSourceExtractComplete": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("table_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("row_count", _TYPE_INT32, _LABEL_OPTIONAL),
        ("elapsed", _TYPE_FLOAT, _LABEL_OPTIONAL),
    ],
    "FederationSourceExtractSkipped": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("table_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("reason", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationSourceExtractError": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("table_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("error", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationPredicatePushdown": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("predicates", _TYPE_STRING, _LABEL_REPEATED),
        ("limit", _TYPE_INT32, _LABEL_OPTIONAL),
    ],
    "FederationTempViewRegistered": [
        ("view_name", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationTempViewCleanup": [
        ("view_count", _TYPE_INT32, _LABEL_OPTIONAL),
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationSQLTranslation": [
        ("source_dialect", _TYPE_STRING, _LABEL_OPTIONAL),
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationSQLTranslationError": [
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("error", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationExecutionStart": [
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("num_sources", _TYPE_INT32, _LABEL_OPTIONAL),
    ],
    "FederationExecutionComplete": [
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("elapsed", _TYPE_FLOAT, _LABEL_OPTIONAL),
    ],
    "FederationExecutionError": [
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("error", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationWriteStart": [
        ("target", _TYPE_STRING, _LABEL_OPTIONAL),
        ("schema", _TYPE_STRING, _LABEL_OPTIONAL),
        ("relation", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationWriteComplete": [
        ("target", _TYPE_STRING, _LABEL_OPTIONAL),
        ("schema", _TYPE_STRING, _LABEL_OPTIONAL),
        ("relation", _TYPE_STRING, _LABEL_OPTIONAL),
        ("row_count", _TYPE_INT32, _LABEL_OPTIONAL),
        ("elapsed", _TYPE_FLOAT, _LABEL_OPTIONAL),
    ],
    "FederationWriteError": [
        ("target", _TYPE_STRING, _LABEL_OPTIONAL),
        ("error", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationMaterializationCoercion": [
        ("model_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("original", _TYPE_STRING, _LABEL_OPTIONAL),
        ("coerced", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationIncrementalExtract": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("new_rows", _TYPE_INT32, _LABEL_OPTIONAL),
        ("changed_rows", _TYPE_INT32, _LABEL_OPTIONAL),
        ("deleted_rows", _TYPE_INT32, _LABEL_OPTIONAL),
    ],
    "FederationSchemaChange": [
        ("source_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("table_name", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationBucketOperation": [
        ("operation", _TYPE_STRING, _LABEL_OPTIONAL),
        ("bucket_name", _TYPE_STRING, _LABEL_OPTIONAL),
        ("path", _TYPE_STRING, _LABEL_OPTIONAL),
    ],
    "FederationSummary": [
        ("federation_models", _TYPE_INT32, _LABEL_OPTIONAL),
        ("pushdown_models", _TYPE_INT32, _LABEL_OPTIONAL),
        ("sources_extracted", _TYPE_INT32, _LABEL_OPTIONAL),
    ],
}


def _build_federation_protos():
    """Build and register protobuf data messages AND *Msg wrapper messages."""
    file_proto = _descriptor_pb2.FileDescriptorProto()
    file_proto.name = "dvt/events/federation_types.proto"
    file_proto.package = "dvt.events"
    file_proto.syntax = "proto3"
    # Declare dependency on the upstream proto so we can reference CoreEventInfo
    file_proto.dependency.append("dbtlabs/proto/public/v1/fields/core_types.proto")

    # 1) Add all data messages
    for msg_name, fields in _FEDERATION_MESSAGES.items():
        msg_desc = file_proto.message_type.add()
        msg_desc.name = msg_name
        for i, (fname, ftype, flabel) in enumerate(fields, start=1):
            fd = msg_desc.field.add()
            fd.name = fname
            fd.number = i
            fd.type = ftype
            fd.label = flabel

    # 2) Add *Msg wrapper messages (info: CoreEventInfo, data: <DataMsg>)
    for msg_name in _FEDERATION_MESSAGES:
        wrapper = file_proto.message_type.add()
        wrapper.name = f"{msg_name}Msg"
        # field 1: info (CoreEventInfo from upstream proto)
        info_fd = wrapper.field.add()
        info_fd.name = "info"
        info_fd.number = 1
        info_fd.type = _TYPE_MESSAGE
        info_fd.label = _LABEL_OPTIONAL
        info_fd.type_name = f".{_CORE_EVENT_INFO_FQN}"
        # field 2: data (the Federation data message)
        data_fd = wrapper.field.add()
        data_fd.name = "data"
        data_fd.number = 2
        data_fd.type = _TYPE_MESSAGE
        data_fd.label = _LABEL_OPTIONAL
        data_fd.type_name = f".dvt.events.{msg_name}"

    _pool.Add(file_proto)

    generated = {}
    for msg_name in _FEDERATION_MESSAGES:
        # Register data message
        desc = _pool.FindMessageTypeByName(f"dvt.events.{msg_name}")
        cls = _reflection.GeneratedProtocolMessageType(
            msg_name,
            tuple(),
            {"DESCRIPTOR": desc, "__module__": __name__},
        )
        _sym_db.RegisterMessage(cls)
        generated[msg_name] = cls

        # Register *Msg wrapper
        wrapper_name = f"{msg_name}Msg"
        wrapper_desc = _pool.FindMessageTypeByName(f"dvt.events.{wrapper_name}")
        wrapper_cls = _reflection.GeneratedProtocolMessageType(
            wrapper_name,
            tuple(),
            {"DESCRIPTOR": wrapper_desc, "__module__": __name__},
        )
        _sym_db.RegisterMessage(wrapper_cls)
        generated[wrapper_name] = wrapper_cls

    return generated


_federation_classes = _build_federation_protos()
globals().update(_federation_classes)
