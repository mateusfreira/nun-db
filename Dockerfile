FROM ekidd/rust-musl-builder:stable as builder
RUN rustup update
RUN USER=root cargo new --bin nun-db
WORKDIR ./nun-db

COPY ./Cargo.toml ./Cargo.toml
RUN mkdir benches/
RUN mkdir src/lib/
RUN mkdir src/bin/
RUN touch benches/nundb_benchmark.rs
RUN touch src/lib/lib.rs
RUN mv src/main.rs src/bin/main.rs
RUN cargo build --release
RUN rm src/**/*.rs
RUN rm benches/*.rs

ADD . ./

#RUN rm ./target/x86_64-unknown-linux-musl/release/deps/nun*
RUN cargo build --release

FROM alpine:3.12.4

RUN apk add libressl-dev
COPY --from=builder /home/rust/src/nun-db/target/x86_64-unknown-linux-musl/release/nun-db /usr/bin/nun-db
ENV NUN_WS_ADDR   "0.0.0.0:3012"
ENV NUN_HTTP_ADDR "0.0.0.0:3013"
ENV NUN_TCP_ADDR  "0.0.0.0:3014"

CMD ["sh" , "-c", "nun-db -u ${NUN_USER} -p ${NUN_PWD} start --http-address ${NUN_HTTP_ADDR} --tcp-address ${NUN_TCP_ADDR} --ws-address ${NUN_WS_ADDR} "]

