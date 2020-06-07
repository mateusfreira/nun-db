FROM rust:1.40 as builder
WORKDIR /usr/src/nun-db
COPY . .
RUN cargo install --path .

FROM debian:buster-slim
RUN apt-get update
COPY --from=builder /usr/local/cargo/bin/nun-db /usr/local/bin/nun-db
CMD ["nun-db", "-u", "$NUN_USER", "-p", "$NUN_PWD", "start"]
