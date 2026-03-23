mod common;

use common::surreal::TestSurrealContainer;
use oversync::distributed_lock::PipeLock;

#[tokio::test]
async fn lock_acquire_and_release() {
	let surreal = TestSurrealContainer::new().await;
	let lock = PipeLock::new(surreal.client.clone(), "instance-1".into());

	let acquired = lock.try_acquire("pipe-a:q1", 60).await.unwrap();
	assert!(acquired);

	lock.release("pipe-a:q1").await.unwrap();
}

#[tokio::test]
async fn lock_prevents_double_acquire() {
	let surreal = TestSurrealContainer::new().await;
	let lock1 = PipeLock::new(surreal.client.clone(), "instance-1".into());
	let lock2 = PipeLock::new(surreal.client.clone(), "instance-2".into());

	let acquired1 = lock1.try_acquire("pipe-a:q1", 300).await.unwrap();
	assert!(acquired1);

	let acquired2 = lock2.try_acquire("pipe-a:q1", 300).await.unwrap();
	assert!(!acquired2, "second instance should NOT acquire lock held by first");
}

#[tokio::test]
async fn lock_same_instance_can_reacquire() {
	let surreal = TestSurrealContainer::new().await;
	let lock = PipeLock::new(surreal.client.clone(), "instance-1".into());

	let acquired1 = lock.try_acquire("pipe-a:q1", 300).await.unwrap();
	assert!(acquired1);

	let acquired2 = lock.try_acquire("pipe-a:q1", 300).await.unwrap();
	assert!(acquired2, "same instance should reacquire its own lock");
}

#[tokio::test]
async fn lock_release_allows_other_instance() {
	let surreal = TestSurrealContainer::new().await;
	let lock1 = PipeLock::new(surreal.client.clone(), "instance-1".into());
	let lock2 = PipeLock::new(surreal.client.clone(), "instance-2".into());

	lock1.try_acquire("pipe-a:q1", 300).await.unwrap();
	lock1.release("pipe-a:q1").await.unwrap();

	let acquired = lock2.try_acquire("pipe-a:q1", 300).await.unwrap();
	assert!(acquired, "after release, other instance should acquire");
}

#[tokio::test]
async fn lock_expired_can_be_taken() {
	let surreal = TestSurrealContainer::new().await;
	let lock1 = PipeLock::new(surreal.client.clone(), "instance-1".into());
	let lock2 = PipeLock::new(surreal.client.clone(), "instance-2".into());

	// Acquire with 1 second TTL
	lock1.try_acquire("pipe-a:q1", 1).await.unwrap();

	// Wait for expiry
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;

	let acquired = lock2.try_acquire("pipe-a:q1", 300).await.unwrap();
	assert!(acquired, "expired lock should be acquirable by other instance");
}

#[tokio::test]
async fn lock_different_pipes_independent() {
	let surreal = TestSurrealContainer::new().await;
	let lock1 = PipeLock::new(surreal.client.clone(), "instance-1".into());
	let lock2 = PipeLock::new(surreal.client.clone(), "instance-2".into());

	let a = lock1.try_acquire("pipe-a:q1", 300).await.unwrap();
	let b = lock2.try_acquire("pipe-b:q1", 300).await.unwrap();

	assert!(a);
	assert!(b, "different pipes should have independent locks");
}
