#
# This is the Dockerfile for a customized Remote Manager Server
#

#
# Stage 1: Build external bundles
#
FROM macchina/reflector-sdk:2.7.0 as buildstage

ADD influxdb /home/reflector/build/influxdb

RUN make -s -C /home/reflector/build/influxdb DEFAULT_TARGET=shared_release

#
# Stage 2: Build Customized Remote Manager
#
FROM macchina/reflector:2.7.0

COPY reflector.license /home/reflector/etc/reflector.license
COPY reflector.properties /home/reflector/etc/reflector.properties
COPY --from=buildstage /home/reflector/build/bundles/* /home/reflector/lib/bundles/

ENV INFLUX_HOST=localhost
ENV INFLUX_PORT=8086
ENV INFLUX_BUCKET=remote_manager
ENV INFLUX_ORG=macchina
ENV INFLUX_TOKEN=influxtoken
