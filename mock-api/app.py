#!/usr/bin/env python3
"""
Mock API for Log Storage and Retrieval
This API stores logs and provides endpoints for retrieving them.
It also stores regex match results from the Spark processing.
"""
from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import logging


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get database URL from environment variable
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/logs_db")

# Cache for storing logs in memory for quick access
log_cache = {}

# Helper function to connect to the database
def get_db_connection():
    retries = 0
    max_retries = 10
    
    while retries < max_retries:
        try:
            connection = psycopg2.connect(DATABASE_URL)
            connection.autocommit = True
            print("Database connection established")
            return connection
        except Exception as e:
            retries += 1
            wait_time = retries * 5
            print(f"Database connection failed: {str(e)}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    print("Could not connect to database after maximum retries")
    return None

@app.route('/logs', methods=['POST'])
def create_log():
    """Create a new log entry"""
    data = request.json
    
    if not data or 'log_id' not in data or 'content' not in data:
        return jsonify({"error": "Missing required fields"}), 400
    
    log_id = data['log_id']
    content = data['content']
    source = data.get('source', 'unknown')
    
    # Store in cache for quick access
    log_cache[log_id] = content
    
    # Store in database
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO logs (log_id, source, content) VALUES (%s, %s, %s) ON CONFLICT (log_id) DO UPDATE SET content = %s",
                    (log_id, source, content, content)
                )
            conn.close()
            return jsonify({"message": "Log created", "log_id": log_id}), 201
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500
    else:
        # Fall back to cache-only operation if DB is unavailable
        return jsonify({"message": "Log cached (DB unavailable)", "log_id": log_id}), 201

@app.route('/logs/<log_id>', methods=['GET'])
def get_log(log_id):
    """Retrieve a log by ID"""
    # Check cache first for efficiency
    if log_id in log_cache:
        return log_cache[log_id]
    
    # If not in cache, check database
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT content FROM logs WHERE log_id = %s", (log_id,))
                result = cur.fetchone()
                
                if result:
                    # Add to cache for future requests
                    log_cache[log_id] = result[0]
                    conn.close()
                    return result[0]
                else:
                    conn.close()
                    return "Log not found", 404
        except Exception as e:
            conn.close()
            return str(e), 500
    else:
        return "Database connection failed", 503

# --- NEW POST API for Bulk Log Retrieval (modified to use IN query) ---
@app.route('/logs/bulk', methods=['POST'])
def get_logs_by_ids():
    """
    Fetches an array of content when given a list of log IDs using a single IN query.
    Expects a JSON payload like: {"log_ids": ["id1", "id2", "id3"]}
    Returns a JSON array of objects: [{"log_id": "id1", "content": "..."}]
    """
    if not request.is_json:
        logger.error("Request is not JSON.")
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()
    log_ids = data.get('log_ids')

    if not isinstance(log_ids, list):
        logger.error("Invalid 'log_ids' format. Expected a list.")
        return jsonify({"error": "'log_ids' must be a list"}), 400

    if not log_ids:
        logger.warning("Received empty list of log_ids.")
        return jsonify([]), 200 # Return empty array if no IDs are provided

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return "Database connection failed", 503

        results_map = {} # Use a map to store results by log_id for easy lookup
        with conn.cursor() as cur:
            # Construct the IN clause dynamically
            placeholders = ','.join(['%s'] * len(log_ids))
            query = f"SELECT log_id, content FROM logs WHERE log_id IN ({placeholders})"
            logger.info(f"Executing bulk query for {len(log_ids)} log IDs.")
            cur.execute(query, tuple(log_ids))
            
            for row in cur.fetchall():
                log_id, content = row
                results_map[log_id] = {"log_id": log_id, "content": content}

        # Build the final list of results, ensuring order and including 'not_found' status
        final_results = []
        for log_id in log_ids:
            if log_id in results_map:
                final_results.append(results_map[log_id])
            else:
                final_results.append({"log_id": log_id, "content": None, "status": "not_found"})
                logger.warning(f"Log_id {log_id} requested in bulk not found in DB.")

        logger.info(f"Processed bulk request for {len(log_ids)} log IDs. Found {len(results_map)} logs.")
        return jsonify(final_results), 200

    except Exception as e:
        logger.error(f"An error occurred during bulk log retrieval: {e}", exc_info=True)
        return str(e), 500
    finally:
        if conn:
            conn.close()

@app.route('/list-logs', methods=['GET'])
def list_logs():
    """List all log IDs"""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT log_id FROM logs where processed = FALSE ORDER BY timestamp DESC LIMIT 100")
                results = [row[0] for row in cur.fetchall()]
                conn.close()
                return jsonify(results)
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500
    else:
        # Fall back to cache if DB is unavailable
        return jsonify(list(log_cache.keys())[:100])

@app.route('/results', methods=['POST'])
def store_results():
    """Store regex match results"""
    results = request.json
    
    if not results:
        return jsonify({"message": "No results to store"}), 200
    
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                for result in results:
                    cur.execute(
                        """
                        INSERT INTO regex_results 
                        (log_id, regex_id, line_number, match, line_content) 
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            result['log_id'], 
                            result['regex_id'], 
                            result['line_number'], 
                            result['match'],
                            result.get('line_content', '')
                        )
                    )
                
                # Update the processed flag on the logs
                log_ids = set(result['log_id'] for result in results)
                for log_id in log_ids:
                    cur.execute(
                        "UPDATE logs SET processed = TRUE WHERE log_id = %s",
                        (log_id,)
                    )
            
            conn.close()
            return jsonify({"message": f"Stored {len(results)} results"}), 201
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Database connection failed"}), 503

@app.route('/results/<log_id>', methods=['GET'])
def get_results(log_id):
    """Get regex match results for a specific log"""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT regex_id, line_number, match, line_content
                    FROM regex_results
                    WHERE log_id = %s
                    ORDER BY line_number, regex_id
                    """,
                    (log_id,)
                )
                results = cur.fetchall()
                conn.close()
                return jsonify(results)
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Database connection failed"}), 503

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get statistics on log processing"""
    conn = get_db_connection()
    if conn:
        try:
            stats = {}
            with conn.cursor() as cur:
                # Total logs
                cur.execute("SELECT COUNT(*) FROM logs")
                stats['total_logs'] = cur.fetchone()[0]
                
                # Processed logs
                cur.execute("SELECT COUNT(*) FROM logs WHERE processed = TRUE")
                stats['processed_logs'] = cur.fetchone()[0]
                
                # Total regex matches
                cur.execute("SELECT COUNT(*) FROM regex_results")
                stats['total_matches'] = cur.fetchone()[0]
                
                # Matches by regex type
                cur.execute(
                    """
                    SELECT regex_id, COUNT(*) 
                    FROM regex_results 
                    GROUP BY regex_id 
                    ORDER BY COUNT(*) DESC
                    """
                )
                stats['matches_by_type'] = {row[0]: row[1] for row in cur.fetchall()}
            
            conn.close()
            return jsonify(stats)
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Database connection failed"}), 503

@app.route('/health', methods=['GET'])
def health_check():
    """API health check endpoint"""
    # Check database connection
    conn = get_db_connection()
    db_status = "connected" if conn else "disconnected"
    
    if conn:
        conn.close()
    
    return jsonify({
        "status": "healthy",
        "database": db_status,
        "cache_size": len(log_cache)
    })

if __name__ == '__main__':
    # Wait for database to be ready before starting
    print("Waiting for database to be ready...")
    time.sleep(10)
    
    # Test database connection on startup
    connection = get_db_connection()
    if connection:
        connection.close()
        print("Database connection successful, API starting up")
    else:
        print("Warning: Database not available, starting in cache-only mode")
    
    # Start the application
    app.run(host='0.0.0.0', port=5000)