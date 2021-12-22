use criterion::{criterion_group, criterion_main, Criterion};
use kvs::engine::*;
use rand::Rng;
use rand::thread_rng;
use rand::distributions::Alphanumeric;
use tempfile::TempDir;

fn format_key_value(c: &mut Criterion) {
	let kvs_dir = TempDir::new().expect("unable to create temporary working directory");
	let kvs = KvStore::open(kvs_dir.path()).unwrap();
	let sled_dir = TempDir::new().expect("unable to create temporary working directory");
	let sled = SledKvsEngine::open(sled_dir.path()).unwrap();
	c.bench_function("kvs write", |b| b.iter(|| {
		for i in 0..10000 {
			assert!(kvs.set(format!("key{}", i), format!("value{}", i)).is_ok());
		}
	}));

	c.bench_function("kvs read", |b| b.iter(|| {
		for i in 0..10000 {
			assert!(kvs.get(format!("key{}", i)).is_ok());
		}
	}));

	c.bench_function("sled write", |b| b.iter(|| {
		for i in 0..100 {
			assert!(sled.set(format!("key{}", i), format!("value{}", i)).is_ok());
		}
	}));

	c.bench_function("sled read", |b| b.iter(|| {
		for i in 0..100 {
			assert!(sled.get(format!("key{}", i)).is_ok());
		}
	}));
}

fn random_generated_key_value(c: &mut Criterion) {
	let mut keys:Vec<String> = vec![];
	let mut values: Vec<String> = vec![];
	println!("Init keys and values...");
	for _ in 0..100 {
		let klen = thread_rng().gen_range(1, 100000);
		let vlen = thread_rng().gen_range(1, 100000);
		keys.push(
			thread_rng()
			.sample_iter(&Alphanumeric)
			.take(klen)
			.collect()
		);

		values.push(
			thread_rng()
			.sample_iter(&Alphanumeric)
			.take(vlen)
			.collect()
		);
	}

	println!("Init OK!");
	println!("Create engines...");
	let kvs_dir = TempDir::new().expect("unable to create temporary working directory");
	let kvs = KvStore::open(kvs_dir.path()).unwrap();
	let sled_dir = TempDir::new().expect("unable to create temporary working directory");
	let sled = SledKvsEngine::open(sled_dir.path()).unwrap();

	println!("Create engines...");
    c.bench_function("kvs write", |b| b.iter(|| {
		for i in 0..100 {
			assert!(kvs.set(keys[i].to_owned(), values[i].to_owned()).is_ok());
		}
	}));

	c.bench_function("kvs read", |b| b.iter(|| {
		for i in 0..100 {
			assert!(kvs.get(keys[i].to_owned()).is_ok());
		}
	}));

	c.bench_function("sled write", |b| b.iter(|| {
		for i in 0..100 {
			sled.set(keys[i].to_owned(), values[i].to_owned()).unwrap();
		}
	}));

	c.bench_function("sled read", |b| b.iter(|| {
		for i in 0..100 {
			sled.get(keys[i].to_owned()).unwrap();
		}
	}));
}

criterion_group!(benches, format_key_value, random_generated_key_value);
criterion_main!(benches);