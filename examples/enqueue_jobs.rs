use backfill::{BackfillClient, JobSpec, Priority, Queue, enqueue_fast, enqueue_bulk};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Example jobs that can be processed by the backfill worker
#[derive(Debug, Serialize, Deserialize)]
pub struct ExampleJob {
    pub message: String,
    pub delay_ms: Option<u64>,
    pub should_fail: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SendEmailJob {
    pub to: String,
    pub subject: String,
    pub body: String,
    pub template: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessUserDataJob {
    pub user_id: String,
    pub data_type: String,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateReportJob {
    pub report_type: String,
    pub date_range: String,
    pub output_format: String,
    pub recipients: Vec<String>,
}

/// Setup tracing for better logging
fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("backfill=info,enqueue_jobs=info"));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_level(true)
                .with_ansi(true),
        )
        .with(filter)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost:5432/backfill".to_string());

    println!("🚀 Connecting to database: {}", database_url);
    
    // Create the backfill client
    let client = BackfillClient::new(&database_url).await?;
    println!("✅ Connected to database and initialized schema");

    println!("\n📝 Enqueueing example jobs...");

    // Enqueue a simple example job
    let example_job = ExampleJob {
        message: "Hello from the example job!".to_string(),
        delay_ms: Some(1000), // 1 second delay
        should_fail: Some(false),
    };

    let job = client
        .enqueue(
            "example_job",
            &example_job,
            JobSpec {
                priority: Priority::BULK_DEFAULT,
                queue: Queue::Custom("default".to_string()),
                job_key: Some("example-1".to_string()),
                ..Default::default()
            },
        )
        .await?;
    println!("✉️  Enqueued ExampleJob: {} (job_id: {})", example_job.message, job.id());

    // Enqueue a high-priority email job using the convenience function
    let email_job = SendEmailJob {
        to: "user@example.com".to_string(),
        subject: "Welcome to Backfill!".to_string(),
        body: "Thanks for trying out the Rust backfill worker.".to_string(),
        template: Some("welcome".to_string()),
    };

    let job = enqueue_fast(
        &client,
        "send_email",
        &email_job,
        Some("welcome-email-user123".to_string()),
    )
    .await?;
    println!("📧 Enqueued SendEmailJob to: {} (job_id: {})", email_job.to, job.id());

    // Enqueue a bulk processing job  
    let process_job = ProcessUserDataJob {
        user_id: "user-456".to_string(),
        data_type: "analytics".to_string(),
        batch_size: Some(100),
    };

    let job = enqueue_bulk(
        &client,
        "process_user_data",
        &process_job,
        Some(format!("process-{}-{}", process_job.user_id, process_job.data_type)),
    )
    .await?;
    println!("📊 Enqueued ProcessUserDataJob for user: {} (job_id: {})", process_job.user_id, job.id());

    // Enqueue a scheduled report generation job (delayed by 30 seconds)
    let report_job = GenerateReportJob {
        report_type: "monthly_summary".to_string(),
        date_range: "2024-09-01 to 2024-09-30".to_string(),
        output_format: "pdf".to_string(),
        recipients: vec!["admin@example.com".to_string(), "manager@example.com".to_string()],
    };

    let job = client
        .enqueue(
            "generate_report",
            &report_job,
            JobSpec {
                priority: Priority::BULK_LOW,
                queue: Queue::Custom("reports".to_string()),
                run_at: Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
                job_key: Some("monthly-summary-2024-09".to_string()),
                ..Default::default()
            },
        )
        .await?;
    println!("📈 Enqueued GenerateReportJob (scheduled for 30s): {} report (job_id: {})", report_job.report_type, job.id());

    // Enqueue a job that will fail (for testing error handling)
    let failing_job = ExampleJob {
        message: "This job will fail to test error handling".to_string(),
        delay_ms: Some(500),
        should_fail: Some(true),
    };

    let job = client
        .enqueue(
            "example_job",
            &failing_job,
            JobSpec {
                priority: Priority::BULK_DEFAULT,
                queue: Queue::Custom("default".to_string()),
                job_key: Some("failing-example".to_string()),
                ..Default::default()
            },
        )
        .await?;
    println!("💥 Enqueued failing ExampleJob for error testing (job_id: {})", job.id());

    println!("\n🎯 All jobs enqueued successfully!");
    println!("\n🔧 To process these jobs, run the worker:");
    println!("   cargo run --bin backfill-worker");
    println!("\n💡 Configure the worker with environment variables:");
    println!("   DATABASE_URL=postgresql://localhost:5432/backfill");
    println!("   RUST_LOG=backfill=info,backfill_worker=info");
    println!("   FAST_QUEUE_CONCURRENCY=5");
    println!("   BULK_QUEUE_CONCURRENCY=3");

    Ok(())
}
