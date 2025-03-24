from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import json
from datetime import datetime
import logging

# Import the IPLScraper class from your updated file
from paste import IPLScraper

# Configure API logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ipl_api.log"),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Store active scrapers in memory
active_scrapers = {}

@app.route('/api/match/<match_id>', methods=['GET'])
def get_match_data(match_id):
    """Get the current data for a specific match."""
    tournament_id = request.args.get('tournament_id', '8307')  # Default to IPL tournament ID
    
    # Create a key for this match
    match_key = f"{match_id}_{tournament_id}"
    
    # Create or get scraper for this match
    if match_key not in active_scrapers:
        try:
            app.logger.info(f"Initializing scraper for match {match_id}")
            scraper = IPLScraper(match_id=match_id, tournament_id=tournament_id)
            scraper.update()  # Fetch initial data
            active_scrapers[match_key] = scraper
        except Exception as e:
            app.logger.error(f"Error initializing scraper: {e}")
            return jsonify({"error": f"Failed to initialize scraper: {str(e)}"}), 500
    else:
        # Get the existing scraper
        scraper = active_scrapers[match_key]
        
        # Update data if requested
        refresh = request.args.get('refresh', 'false').lower() == 'true'
        if refresh:
            try:
                app.logger.info(f"Refreshing data for match {match_id}")
                scraper.update()
            except Exception as e:
                app.logger.error(f"Error updating match data: {e}")
                return jsonify({"error": f"Failed to update match data: {str(e)}"}), 500
    
    # Return the match data
    return jsonify(scraper.match_data)

@app.route('/api/match/<match_id>/refresh', methods=['POST'])
def refresh_match_data(match_id):
    """Force refresh the data for a specific match."""
    tournament_id = request.json.get('tournament_id', '8307')
    match_key = f"{match_id}_{tournament_id}"
    
    if match_key not in active_scrapers:
        # Create new scraper if it doesn't exist
        try:
            app.logger.info(f"Initializing scraper for match {match_id}")
            scraper = IPLScraper(match_id=match_id, tournament_id=tournament_id)
            scraper.update()
            active_scrapers[match_key] = scraper
        except Exception as e:
            app.logger.error(f"Error initializing scraper: {e}")
            return jsonify({"error": f"Failed to initialize scraper: {str(e)}"}), 500
    else:
        # Update existing scraper
        try:
            app.logger.info(f"Refreshing data for match {match_id}")
            active_scrapers[match_key].update()
        except Exception as e:
            app.logger.error(f"Error updating match data: {e}")
            return jsonify({"error": f"Failed to update match data: {str(e)}"}), 500
    
    return jsonify({
        "status": "success", 
        "message": f"Match data refreshed for match {match_id}",
        "last_updated": active_scrapers[match_key].match_data['last_updated']
    })

@app.route('/api/matches', methods=['GET'])
def list_matches():
    """List all currently tracked matches."""
    result = []
    for match_key, scraper in active_scrapers.items():
        match_id, tournament_id = match_key.split('_')
        match_info = {
            "match_id": match_id,
            "tournament_id": tournament_id,
            "title": scraper.match_data['match_info']['title'],
            "status": scraper.match_data['match_info']['status'],
            "teams": {
                "team1": scraper.match_data['teams'].get('team1', {}).get('name', 'Unknown'),
                "team2": scraper.match_data['teams'].get('team2', {}).get('name', 'Unknown'),
            },
            "scores": {
                "team1": scraper.match_data['teams'].get('team1', {}).get('score', 'N/A'),
                "team2": scraper.match_data['teams'].get('team2', {}).get('score', 'N/A'),
            },
            "last_updated": scraper.match_data['last_updated']
        }
        result.append(match_info)
    
    return jsonify(result)

@app.route('/api/match/<match_id>/commentary', methods=['GET'])
def get_commentary(match_id):
    """Get only the commentary for a specific match."""
    tournament_id = request.args.get('tournament_id', '8307')
    match_key = f"{match_id}_{tournament_id}"
    
    # Initialize the match if it doesn't exist
    if match_key not in active_scrapers:
        try:
            app.logger.info(f"Initializing scraper for match {match_id}")
            scraper = IPLScraper(match_id=match_id, tournament_id=tournament_id)
            scraper.update()  # Fetch initial data
            active_scrapers[match_key] = scraper
        except Exception as e:
            app.logger.error(f"Error initializing scraper: {e}")
            return jsonify({"error": f"Failed to initialize scraper: {str(e)}"}), 500
    
    refresh = request.args.get('refresh', 'false').lower() == 'true'
    if refresh:
        try:
            app.logger.info(f"Refreshing data for match {match_id} commentary")
            active_scrapers[match_key].update()
        except Exception as e:
            app.logger.error(f"Error updating match data: {e}")
            return jsonify({"error": f"Failed to update match data: {str(e)}"}), 500
    
    # Get commentary and only return that portion
    commentary = active_scrapers[match_key].match_data.get('commentary', [])
    return jsonify(commentary)

@app.route('/api/match/<match_id>/scorecard', methods=['GET'])
def get_scorecard(match_id):
    """Get only the scorecard for a specific match."""
    tournament_id = request.args.get('tournament_id', '8307')
    match_key = f"{match_id}_{tournament_id}"
    
    # Initialize the match if it doesn't exist
    if match_key not in active_scrapers:
        try:
            app.logger.info(f"Initializing scraper for match {match_id}")
            scraper = IPLScraper(match_id=match_id, tournament_id=tournament_id)
            scraper.update()  # Fetch initial data
            active_scrapers[match_key] = scraper
        except Exception as e:
            app.logger.error(f"Error initializing scraper: {e}")
            return jsonify({"error": f"Failed to initialize scraper: {str(e)}"}), 500
    
    refresh = request.args.get('refresh', 'false').lower() == 'true'
    if refresh:
        try:
            app.logger.info(f"Refreshing data for match {match_id} scorecard")
            active_scrapers[match_key].update()
        except Exception as e:
            app.logger.error(f"Error updating match data: {e}")
            return jsonify({"error": f"Failed to update match data: {str(e)}"}), 500
    
    scraper = active_scrapers[match_key]
    
    # Get match data
    match_data = scraper.match_data
    
    # Process data to ensure correctness
    teams_data = match_data['teams']
    
    # Determine match state (first innings, second innings, completed)
    match_state = "in_progress"
    batting_team = None
    bowling_team = None
    
    team1_has_batted = teams_data.get('team1', {}).get('score', '').lower() != 'yet to bat'
    team2_has_batted = teams_data.get('team2', {}).get('score', '').lower() != 'yet to bat'
    
    if team1_has_batted and not team2_has_batted:
        # First innings (team1 batting, team2 bowling)
        match_state = "first_innings"
        batting_team = "team1"
        bowling_team = "team2"
    elif team1_has_batted and team2_has_batted:
        # Second innings or completed
        match_state = "second_innings"
        batting_team = "team2"
        bowling_team = "team1"
        
        # Improved match completion detection
        match_status = match_data['match_info'].get('status', '').lower()
        if ('won by' in match_status or 
            'match tied' in match_status or 
            'won the match' in match_status or
            'match over' in match_status or
            any(team.get('won') for team in teams_data.values())):
            match_state = "completed"
    
    # Create a focused and corrected scorecard response
    scorecard = {
        "match_info": match_data['match_info'],
        "teams": match_data['teams'],
        "match_state": match_state,
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "batting_stats": {},
        "bowling_stats": {},
        "last_updated": match_data['last_updated']
    }
    
    # Only include batting stats for teams that have actually batted
    for team_key, batting_data in match_data['batting_stats'].items():
        if team_key in teams_data and teams_data[team_key].get('score', '').lower() != 'yet to bat':
            scorecard['batting_stats'][team_key] = batting_data
    
    # Only include bowling stats for teams that have actually bowled
    for team_key, bowling_data in match_data['bowling_stats'].items():
        # In cricket, if team X has batted, then team Y was bowling
        opposing_team = "team2" if team_key == "team1" else "team1"
        if opposing_team in teams_data and teams_data[opposing_team].get('score', '').lower() != 'yet to bat':
            scorecard['bowling_stats'][team_key] = bowling_data
    
    return jsonify(scorecard)

@app.route('/api/match/<match_id>/debug', methods=['GET'])
def get_debug_info(match_id):
    """Get debug information for a specific match (admin only)."""
    # Check for admin authorization
    api_key = request.headers.get('X-API-Key')
    if not api_key or api_key != os.environ.get('ADMIN_API_KEY', 'demo_admin_key'):
        return jsonify({"error": "Unauthorized access"}), 401
    
    tournament_id = request.args.get('tournament_id', '8307')
    match_key = f"{match_id}_{tournament_id}"
    
    if match_key not in active_scrapers:
        return jsonify({"error": "Match not found"}), 404
    
    scraper = active_scrapers[match_key]
    
    # Get the debug log files
    debug_files = []
    debug_dir = scraper.debug_dir
    if os.path.exists(debug_dir):
        for file in os.listdir(debug_dir):
            if file.startswith(f"raw_html_{match_id}"):
                file_path = os.path.join(debug_dir, file)
                stats = os.stat(file_path)
                debug_files.append({
                    "filename": file,
                    "size": stats.st_size,
                    "created": datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S')
                })
    
    # Return debug info
    debug_info = {
        "match_id": match_id,
        "tournament_id": tournament_id,
        "debug_files": debug_files,
        "scraper_stats": {
            "init_time": datetime.fromtimestamp(os.path.getctime(scraper.log_dir)).strftime('%Y-%m-%d %H:%M:%S') if os.path.exists(scraper.log_dir) else "Unknown",
            "last_updated": scraper.match_data['last_updated'],
            "batting_stats_count": {k: len(v) for k, v in scraper.match_data['batting_stats'].items()},
            "bowling_stats_count": {k: len(v) for k, v in scraper.match_data['bowling_stats'].items()},
            "commentary_count": len(scraper.match_data['commentary'])
        }
    }
    
    return jsonify(debug_info)

# Simple home page with API documentation
@app.route('/', methods=['GET'])
def home():
    return """
    <html>
        <head>
            <title>IPL Cricket Match API</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
                h1 { color: #004d99; }
                h2 { color: #0066cc; margin-top: 30px; }
                code { background-color: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
                pre { background-color: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }
                .endpoint { margin-bottom: 20px; padding: 10px; border-left: 3px solid #0066cc; }
                .method { font-weight: bold; color: #004d99; }
                .url { color: #0066cc; }
            </style>
        </head>
        <body>
            <h1>IPL Cricket Match API</h1>
            <p>Access live IPL cricket match data via REST API.</p>
            
            <h2>Endpoints</h2>
            
            <div class="endpoint">
                <h3>Get Match Data</h3>
                <p><span class="method">GET</span> <span class="url">/api/match/{match_id}?tournament_id={tournament_id}&refresh=true|false</span></p>
                <p>Retrieves complete match data including teams, scores, batting stats, bowling stats, and commentary.</p>
            </div>
            
            <div class="endpoint">
                <h3>Force Refresh Match Data</h3>
                <p><span class="method">POST</span> <span class="url">/api/match/{match_id}/refresh</span></p>
                <pre>Content-Type: application/json

{
  "tournament_id": "8307"
}</pre>
                <p>Forces an immediate refresh of the match data from the source.</p>
            </div>
            
            <div class="endpoint">
                <h3>List All Tracked Matches</h3>
                <p><span class="method">GET</span> <span class="url">/api/matches</span></p>
                <p>Lists all matches currently being tracked by the API with basic information.</p>
            </div>
            
            <div class="endpoint">
                <h3>Get Match Commentary</h3>
                <p><span class="method">GET</span> <span class="url">/api/match/{match_id}/commentary?tournament_id={tournament_id}&refresh=true|false</span></p>
                <p>Returns only the commentary section of the match data.</p>
            </div>
            
            <div class="endpoint">
                <h3>Get Match Scorecard</h3>
                <p><span class="method">GET</span> <span class="url">/api/match/{match_id}/scorecard?tournament_id={tournament_id}&refresh=true|false</span></p>
                <p>Returns only the scorecard section of the match data including batting and bowling stats.</p>
            </div>
            
            <h2>Finding Match IDs</h2>
            <p>To find match IDs, go to Bing Cricket and look for the GameId parameter in the URL of the match page.</p>
            <p>The default IPL tournament ID is 8307.</p>
        </body>
    </html>
    """

if __name__ == '__main__':
    # Create log and debug directories if they don't exist
    os.makedirs('match_logs', exist_ok=True)
    os.makedirs('debug_html', exist_ok=True)
    
    # Set admin API key from environment or use default for demo
    if 'ADMIN_API_KEY' not in os.environ:
        os.environ['ADMIN_API_KEY'] = 'demo_admin_key'
        print("WARNING: Using default admin API key. Set ADMIN_API_KEY environment variable for production.")
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)