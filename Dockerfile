FROM rust:1.91.1-bookworm AS builder
RUN apt-get update && apt-get -y install libssl-dev pkg-config
RUN cargo new --bin nun-db
WORKDIR /nun-db
COPY ./Cargo.toml ./Cargo.toml
RUN mkdir -p benches/ src/lib/ src/bin/
RUN touch benches/nundb_disk_benchmark.rs
RUN touch src/lib/lib.rs
RUN mv src/main.rs src/bin/main.rs
RUN cargo build --release
RUN rm -rf src/ benches/

ADD . ./
RUN touch src/bin/main.rs
RUN touch src/lib/lib.rs

RUN cargo build --release

FROM bitnami/minideb:bookworm
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

RUN apt-get update && apt-get -y install libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
COPY --from=builder /nun-db/target/release/nun-db /usr/bin/nun-db
ENV NUN_WS_ADDR="0.0.0.0:3012"
ENV NUN_HTTP_ADDR="0.0.0.0:3013"
ENV NUN_TCP_ADDR="0.0.0.0:3014"

CMD ["sh" , "-c", "nun-db -u ${NUN_USER} -p ${NUN_PWD} start --http-address ${NUN_HTTP_ADDR} --tcp-address ${NUN_TCP_ADDR} --ws-address ${NUN_WS_ADDR} "]

