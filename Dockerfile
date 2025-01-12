FROM rust:1.81.0-slim as builder
RUN apt-get update
RUN apt-get -y install libssl-dev pkg-config
RUN cargo new --bin nun-db
WORKDIR ./nun-db
COPY ./Cargo.toml ./Cargo.toml
RUN mkdir benches/
RUN mkdir src/lib/
RUN mkdir src/bin/
RUN touch benches/nundb_disk_benchmark.rs
RUN touch src/lib/lib.rs
RUN mv src/main.rs src/bin/main.rs
RUN cargo build --release
RUN rm src/**/*.rs
RUN rm benches/*.rs

ADD . ./
RUN touch src/bin/main.rs
RUN touch src/lib/lib.rs

RUN cargo build --release

FROM bitnami/minideb:bookworm
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

RUN apt-get update
RUN apt-get -y install libssl-dev pkg-config
COPY --from=builder ./nun-db/target/release/nun-db /usr/bin/nun-db
ENV NUN_WS_ADDR   "0.0.0.0:3012"
ENV NUN_HTTP_ADDR "0.0.0.0:3013"
ENV NUN_TCP_ADDR  "0.0.0.0:3014"

CMD ["sh" , "-c", "nun-db -u ${NUN_USER} -p ${NUN_PWD} start --http-address ${NUN_HTTP_ADDR} --tcp-address ${NUN_TCP_ADDR} --ws-address ${NUN_WS_ADDR} "]

