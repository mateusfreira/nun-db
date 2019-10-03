FROM rust:1.31.1

WORKDIR /usr/src/friar-db
COPY ./ .

RUN cargo install

CMD ["freira-db"]
EXPOSE 3012
