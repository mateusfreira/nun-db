use criterion::{criterion_group, criterion_main, Criterion};
use nundb::bo::*;
use nundb::disk_ops::*;
use std::env;

fn nundb_disk_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_op_log_group");
    //group.significance_level(0.01).sample_size(100000);
    //group.sampling_mode(criterion::SamplingMode::Linear);
    group.sample_size(10);
    group.bench_function("write_op_log", |b| {
        env::set_var("NUN_MAX_OP_LOG_SIZE", "12500000");
        clean_op_log_metadata_files();
        let mut oplog_file = get_log_file_append_mode();
        b.iter(|| {
            env::set_var("NUN_MAX_OP_LOG_SIZE", "12500000");
            try_write_op_log(
                &mut oplog_file,
                1,
                1,
                &ReplicateOpp::Update,
                Databases::next_op_log_id(),
            ); // Will free f and close the resource ..
        });
    });
}
criterion_group!(benches, nundb_disk_benchmark);
criterion_main!(benches);
