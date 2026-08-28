bootstrap:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	mkdir -p .gen
	.venv/bin/python -m grpc_tools.protoc -I proto --python_out=.gen/ --grpc_python_out=.gen/ \
		proto/envoy/annotations/deprecation.proto \
		proto/envoy/config/core/v3/address.proto \
		proto/envoy/config/core/v3/backoff.proto \
		proto/envoy/config/core/v3/base.proto \
		proto/envoy/config/core/v3/extension.proto \
		proto/envoy/config/core/v3/http_uri.proto \
		proto/envoy/config/core/v3/socket_option.proto \
		proto/envoy/extensions/filters/http/ext_proc/v3/processing_mode.proto \
		proto/envoy/service/ext_proc/v3/external_processor.proto \
		proto/envoy/type/v3/http_status.proto \
		proto/envoy/type/v3/percent.proto \
		proto/envoy/type/v3/semantic_version.proto \
		proto/udpa/annotations/migrate.proto \
		proto/udpa/annotations/status.proto \
		proto/udpa/annotations/versioning.proto \
		proto/validate/validate.proto \
		proto/xds/annotations/v3/status.proto \
		proto/xds/core/v3/context_params.proto
