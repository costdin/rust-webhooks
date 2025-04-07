use std::{
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use postgres::{NoTls, types::ToSql};
use serde_json::Value;
use tokio::time::sleep;
use tokio_postgres::Config;
use serde::Deserialize;
use config::File;

// This struct will be used to share the SQLite connection across handlers
struct AppState {
    db: Pool<PostgresConnectionManager<NoTls>>,
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    // Read configuration
    let config = config::Config::builder()
        .add_source(File::with_name("config.toml"))
        .build()
        .unwrap();
    
    let db_config: DatabaseConfig = match config.try_deserialize() {
        Ok(r) => r,
        Err(e) => { 
            panic!("Invalid configuration: {:#?}", e);
        }
    };
    
    // Initialize the DB and set up shared state
    let data = match init_db_psql(&db_config.host, &db_config.user, &db_config.password, &db_config.dbname).await {
        Ok(db) => web::Data::new(AppState { db }),
        Err(_) => panic!("Error occurred on DB creation"),
    };

    // Set up the Actix web server
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/webhook", web::post().to(store))
            .route("/webhook/{id}", web::get().to(get))
            .route("/missing_webhooks", web::get().to(get_missing))
    })
    .client_request_timeout(std::time::Duration::from_secs(60))
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

async fn init_db_psql(
    host: &str,
    username: &str,
    password: &str,
    dbname: &str,
) -> Result<Pool<PostgresConnectionManager<NoTls>>, tokio_postgres::Error> {
    create_db_psql(host, username, password, dbname).await?;

    // Create the PostgreSQL connection config
    let mut config = Config::new();
    config
        .host(host)
        .user(username)
        .password(password)
        .dbname(dbname);

    // Now, use the config to create a connection manager
    let manager = PostgresConnectionManager::new(config, NoTls);

    // Create the connection pool
    let pool = Pool::builder().max_size(80).build(manager).await?;

    // Get a connection from the pool
    let conn = pool.get().await.unwrap();

    // Create table if not exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS webhooks (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT,
            body JSONB
        )",
        &[], // Empty list of parameters
    )
    .await?;

    // Create index on changelog.id inside JSONB
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_webhooks_changelog_id 
        ON webhooks (((body->'changelog'->>'id')::int))",
        &[],
    )
    .await?;

    Ok(pool.clone())
}

async fn create_db_psql(
    host: &str,
    username: &str,
    password: &str,
    dbname: &str,
) -> Result<(), tokio_postgres::Error> {
    let conn_str = format!("host={host} user={username} password={password}");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    tokio::spawn(connection);

    // Check if the database exists, if not, create it
    let check_db_exists_query = format!("SELECT 1 FROM pg_database WHERE datname = '{}'", dbname);
    let result = client.query_opt(check_db_exists_query.as_str(), &[]).await;

    match result {
        Ok(Some(_)) => {
            // Database exists, no action needed
            println!("Database {} already exists", dbname);
        }
        Ok(None) => {
            // Database doesn't exist, create it
            println!("Database {} does not exist, creating it now...", dbname);
            client
                .batch_execute(&format!("CREATE DATABASE {}", dbname))
                .await?;
        }
        Err(e) => {
            // Handle any error during the query
            return Err(e);
        }
    };

    Ok(())
}

async fn store(payload: web::Json<Value>, data: web::Data<AppState>) -> impl Responder {
    let conn = match data.db.get().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let body = payload.into_inner();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Unexpected error, time went backwards?")
        .as_millis();

    let result = retry_operation(3, Duration::from_millis(20), async || {
        conn.execute(
            "INSERT INTO webhooks (timestamp, body) VALUES ($1, $2)",
            &[&(timestamp as i64), &body],
        )
        .await
    })
    .await;

    match result {
        Ok(_) => HttpResponse::Created().finish(),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

async fn get_missing(
    changelog_ids: web::Json<Vec<i32>>,
    data: web::Data<AppState>,
) -> impl Responder {
    let conn = match data.db.get().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let ids = changelog_ids.into_inner();

    if ids.is_empty() {
        return HttpResponse::BadRequest().body("No IDs provided");
    }

    let query = "
        SELECT (body->'changelog'->>'id')::int as id
        FROM webhooks
        WHERE (body->'changelog'->>'id')::int = ANY($1)
    ";

    let param: &[&(dyn ToSql + Sync)] = &[&ids];

    let rows = match conn.query(query, param).await {
        Ok(rows) => rows,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    // Extract found IDs into a HashSet
    let found_ids: HashSet<i32> = rows.into_iter().map(|row| row.get::<_, i32>(0)).collect();

    // Find missing IDs by comparing to input
    let missing_ids: HashSet<i32> = ids
        .into_iter()
        .filter(|id| !found_ids.contains(id))
        .collect();

    HttpResponse::Ok()
        .content_type("application/json")
        .json(missing_ids)
}

async fn get(changelog_id: web::Path<i32>, data: web::Data<AppState>) -> impl Responder {
    let conn = match data.db.get().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let changelog_id = changelog_id.into_inner();

    let query = "
        SELECT body 
        FROM webhooks 
        WHERE (body->'changelog'->>'id')::int = $1
    ";

    // Execute the query
    let row = conn.query_one(query, &[&(changelog_id as i32)]).await;

    match row {
        Ok(row) => {
            // If the row exists, extract the 'body' JSONB column
            let body: Value = row.get(0);

            // Return the body as a JSON response
            HttpResponse::Ok()
                .content_type("application/json")
                .json(body)
        }
        Err(_) => {
            // If no result is found, return 404 Not Found
            HttpResponse::NotFound().finish()
        }
    }
}

async fn retry_operation<T, Fut, F>(
    retries: usize,
    delay: Duration,
    operation: F,
) -> Result<T, tokio_postgres::error::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, tokio_postgres::error::Error>>,
{
    let mut attempt = 0;

    loop {
        attempt += 1;
        let result = operation().await;

        match result {
            Ok(value) => return Ok(value),
            Err(e) if attempt < retries => {
                eprintln!("Retry attempt {} failed: {:?}", attempt, e);
                sleep(delay).await; // Delay between retries
            }
            Err(e) => {
                eprintln!("Operation failed after {} retries: {:?}", attempt, e);
                return Err(e); // Return the error if retries are exhausted
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    host: String,
    user: String,
    password: String,
    dbname: String,
}
