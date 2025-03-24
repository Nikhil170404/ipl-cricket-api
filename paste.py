# Save this file as paste.py
import requests
from bs4 import BeautifulSoup
import time
import re
import json
import random
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ipl_scraper.log"),
        logging.StreamHandler()
    ]
)

class IPLScraper:
    """A class to scrape live IPL match data from Bing cricket details."""
    
    def __init__(self, match_id=None, tournament_id=None, update_interval=10):
        """
        Initialize the scraper with match details and update interval.
        
        Args:
            match_id (str): Match ID for the specific match
            tournament_id (str): Tournament ID for IPL
            update_interval (int): Seconds between updates (default: 10)
        """
        # Set default tournament ID for IPL 2025
        self.tournament_id = tournament_id or "8307"  # Default IPL tournament ID
        
        # If match ID not provided, use a known recent match
        self.match_id = match_id or "253699"  # Default to a recent IPL match
        
        self.update_interval = update_interval
        
        # Base URL for Bing cricket details
        self.base_url = "https://www.bing.com/cricketdetails"
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.bing.com/',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        self.cookies = {}
        self.match_data = {
            'match_info': {
                'title': '',
                'venue': '',
                'date': '',
                'status': '',
                'result': '',
                'match_id': match_id,
                'tournament_id': tournament_id
            },
            'teams': {},
            'batting_stats': {},
            'bowling_stats': {},
            'commentary': [],
            'last_updated': ''
        }
        
        # Create directories for logs and debug info
        self.log_dir = 'match_logs'
        self.debug_dir = 'debug_html'
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.debug_dir, exist_ok=True)
    
    def construct_url(self):
        """Construct the URL for the cricket details API."""
        # Build a URL that's known to work with Bing's API
        url = (f"{self.base_url}?q=IPL&IsCricketV3=1&ResponseType=FullScore&"
               f"CricketTournamentId={self.tournament_id}&GameId={self.match_id}&"
               f"Provider=SI&ScenarioName=SingleGame&Intent=Schedule&Lang=English&"
               f"QueryTimeZoneId=India Standard Time")
        
        logging.info(f"Constructed URL: {url}")
        return url
    
    def fetch_data(self):
        """Fetch the HTML content from the Bing cricket details page."""
        try:
            # Add a small random delay to avoid rate limiting
            time.sleep(random.uniform(0.5, 1.5))
            
            url = self.construct_url()
            logging.info(f"Fetching data from: {url}")
            
            response = requests.get(url, headers=self.headers, cookies=self.cookies)
            
            # Update cookies for subsequent requests
            self.cookies.update(response.cookies.get_dict())
            
            # Check if response is successful
            response.raise_for_status()
            
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            return None
    
    def save_debug_html(self, html_content):
        """Save the raw HTML for debugging purposes."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.debug_dir}/raw_html_{self.match_id}_{timestamp}.html"
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logging.info(f"Debug HTML saved to {filename}")
            return filename
        except Exception as e:
            logging.error(f"Error saving debug HTML: {e}")
            return None
    
    def parse_match_info(self, soup):
        """Extract basic match information."""
        try:
            # Get match title and tournament info
            tournament_elem = soup.select_one('.ckt_tournamentname, .ckt_match_sbtl')
            if tournament_elem:
                self.match_data['match_info']['title'] = tournament_elem.text.strip()
                logging.info(f"Tournament: {self.match_data['match_info']['title']}")
            
            # Get match status
            status_elem = soup.select_one('.ckt_match_statustxt')
            if status_elem:
                self.match_data['match_info']['status'] = status_elem.text.strip()
                logging.info(f"Match status: {self.match_data['match_info']['status']}")
            
            # Get match date
            date_elem = soup.select_one('.ckt_live_status_text, .b_floatR')
            if date_elem and not date_elem.select_one('.team_score'):
                self.match_data['match_info']['date'] = date_elem.text.strip()
                logging.info(f"Match date: {self.match_data['match_info']['date']}")
            
            # Get player of the match
            mom_elem = soup.select_one('.ckt_match_mom_player')
            if mom_elem:
                self.match_data['match_info']['player_of_match'] = mom_elem.text.strip()
                logging.info(f"Player of the match: {self.match_data['match_info']['player_of_match']}")
            
            # Get venue
            venue_elem = soup.select_one('.ckt_match_venue')
            if venue_elem:
                self.match_data['match_info']['venue'] = venue_elem.text.strip()
                logging.info(f"Venue: {self.match_data['match_info']['venue']}")
        
        except Exception as e:
            logging.error(f"Error parsing match info: {e}")
    
    def parse_teams_and_scores(self, soup):
        """Extract team names and scores."""
        try:
            # Get team details
            team_sections = soup.select('.ckt_match_details, .b_clearfix.ckt_match_details')
            
            for i, section in enumerate(team_sections[:2]):
                team_key = f'team{i+1}'
                
                # Get team name
                name_elem = section.select_one('.ckt_match_teamname')
                if name_elem:
                    team_name = name_elem.text.strip()
                    self.match_data['teams'][team_key] = {'name': team_name}
                    logging.info(f"Team {i+1}: {team_name}")
                
                # Get team score
                score_elem = section.select_one('.team_score, .b_floatR.team_score')
                if score_elem:
                    score_text = score_elem.text.strip()
                    
                    # Check if team hasn't batted yet
                    if 'yet to bat' in score_text.lower():
                        self.match_data['teams'][team_key].update({
                            'score': 'Yet to bat',
                            'runs': 'Yet to bat',
                            'wickets': '0',
                            'overs': ''
                        })
                        logging.info(f"Team {i+1} has not batted yet")
                        continue
                    
                    # Parse score and overs
                    score_parts = score_text.split('(')
                    score = score_parts[0].strip()
                    overs = score_parts[1].replace(')', '').strip() if len(score_parts) > 1 else ''
                    
                    # Parse runs and wickets from score
                    if '/' in score:
                        runs, wickets = score.split('/')
                    else:
                        runs, wickets = score, '0'
                    
                    # Check if innings is complete (20 overs or all out)
                    innings_complete = False
                    if overs and ('20' in overs or float(overs.split()[0]) >= 20.0):
                        innings_complete = True
                        
                    # All out (10 wickets down)
                    if wickets == '10':
                        innings_complete = True
                    
                    self.match_data['teams'][team_key].update({
                        'score': score,
                        'runs': runs,
                        'wickets': wickets,
                        'overs': overs,
                        'innings_complete': innings_complete
                    })
                    
                    if innings_complete:
                        logging.info(f"Team {i+1} innings complete: {score} ({overs})")
                    else:
                        logging.info(f"Team {i+1} score: {score} ({overs})")
                    
                # Check if team won
                if name_elem and 'ckt_won' in name_elem.get('class', []):
                    self.match_data['teams'][team_key]['won'] = True
                    logging.info(f"Team {i+1} won the match")
                    
                # Also check if score has won class
                if score_elem and 'ckt_won' in score_elem.get('class', []):
                    self.match_data['teams'][team_key]['won'] = True
                    logging.info(f"Team {i+1} won the match (from score element)")
        
        except Exception as e:
            logging.error(f"Error parsing teams and scores: {e}")
    
    def parse_batting_stats(self, soup):
        """Extract batting statistics for both teams."""
        try:
            # Clear existing batting stats
            self.match_data['batting_stats'] = {}
            
            # First look for the tab containing the scorecard
            tab_content = soup.select_one('#tab_1, .ckt_fltr_1, div[data-id="ckt_fltr_1"]')
            
            # If tab content is not found, try finding batting tables directly
            batting_tables = []
            if tab_content:
                batting_tables = tab_content.select('.ckt_batsmen, .b_scard table')
            
            # If not found in tab content, search in the entire document
            if not batting_tables:
                batting_tables = soup.select('.ckt_batsmen, .b_scard table')
            
            # Get match status to determine teams' batting order
            match_status = self.match_data['match_info'].get('status', '').lower()
            team1_name = self.match_data['teams'].get('team1', {}).get('name', '').lower()
            team2_name = self.match_data['teams'].get('team2', {}).get('name', '').lower()
            
            # Determine which team is batting first based on match status
            batting_first_team = 'team1'  # Default assumption
            batting_second_team = 'team2'
            
            # Check if team2 is batting first based on match status
            if team2_name and team2_name in match_status and 'elected to field' in match_status:
                batting_first_team = 'team2'
                batting_second_team = 'team1'
            
            batting_count = 0
            for table in batting_tables:
                # Check if this is a batting table by looking for column headers
                header_row = table.select_one('tr.ckt_row_hdr')
                if not header_row or 'BATTERS' not in header_row.text:
                    continue
                
                # Determine team key based on batting order
                team_key = batting_first_team if batting_count == 0 else batting_second_team
                
                # Check if team has actually batted
                team_data = self.match_data['teams'].get(team_key, {})
                score_text = team_data.get('score', '')
                
                # Skip if team hasn't batted yet
                if 'yet to bat' in score_text.lower():
                    continue
                
                self.match_data['batting_stats'][team_key] = []
                all_batsmen = []
                
                # Get all batsman rows
                batsman_rows = table.select('tr.ckt_row_item')
                
                for row in batsman_rows:
                    # Skip if not a valid batsman row
                    name_cell = row.select_one('td:first-child')
                    if not name_cell:
                        continue
                    
                    # Get player name
                    player_link = name_cell.select_one('a')
                    player_name = player_link.text.strip() if player_link else name_cell.text.strip()
                    
                    # Skip if it's a total or extra row
                    if any(keyword in player_name.lower() for keyword in ['total', 'extras']):
                        continue
                    
                    # Get statistics
                    stat_cells = row.select('td')
                    if len(stat_cells) >= 6:
                        runs = stat_cells[1].text.strip()
                        balls = stat_cells[2].text.strip()
                        fours = stat_cells[3].text.strip()
                        sixes = stat_cells[4].text.strip()
                        strike_rate = stat_cells[5].text.strip()
                        
                        # Initially assume not out
                        dismissal = 'not out'
                        is_out = False
                        
                        # Look for signs this batsman is out
                        # 1. Check dismissal text in the footnote
                        next_row = row.find_next_sibling('tr')
                        if next_row and next_row.select_one('.ckt_row_subl, .b_footnote'):
                            dismissal_elem = next_row.select_one('.ckt_row_subl, .b_footnote')
                            if dismissal_elem and dismissal_elem.text.strip():
                                dismissal_text = dismissal_elem.text.strip()
                                if 'not out' not in dismissal_text.lower():
                                    dismissal = dismissal_text
                                    is_out = True
                        
                        # 2. Check for formatting indicating dismissal
                        if 'bold' in name_cell.get('class', []) or 'ckt_dis' in name_cell.get('class', []):
                            is_out = True
                            if dismissal == 'not out':  # Only update if we don't have specific text
                                dismissal = 'out'
                        
                        # 3. Check if there is strikethrough or special formatting
                        if name_cell.select_one('s, strike, del'):
                            is_out = True
                            if dismissal == 'not out':
                                dismissal = 'out'
                        
                        # Track row data for validation pass
                        all_batsmen.append({
                            'name': player_name,
                            'runs': runs,
                            'balls': balls,
                            'fours': fours,
                            'sixes': sixes,
                            'strike_rate': strike_rate,
                            'dismissal': dismissal,
                            'is_out': is_out,
                            'row': row
                        })
                
                # Cross-check with wickets count
                # If wickets count from the score doesn't match our detection, we need to fix it
                wickets_down = int(team_data.get('wickets', '0'))
                out_batsmen = sum(1 for b in all_batsmen if b['is_out'])
                
                # If we missed some dismissals, try to infer who is out
                if wickets_down > out_batsmen:
                    # Sort by position in the batting order (rows higher in the table are earlier batsmen)
                    all_batsmen.sort(key=lambda b: batsman_rows.index(b['row']) if b['row'] in batsman_rows else 999)
                    
                    # Mark batsmen as out from the top until we match the wickets count
                    # Skip current batsmen (usually the last 2 in the list who have lowest scores)
                    current_batsmen = sorted(all_batsmen, key=lambda b: int(b['runs']), reverse=True)[:2]
                    for batsman in all_batsmen:
                        # Skip if already marked as out or if they're one of the current batsmen
                        if batsman['is_out'] or batsman in current_batsmen:
                            continue
                        
                        # Mark as out
                        batsman['is_out'] = True
                        batsman['dismissal'] = 'out'  # Generic dismissal
                        
                        # Break if we've marked enough batsmen as out
                        out_batsmen += 1
                        if out_batsmen >= wickets_down:
                            break
                
                # Add all batsmen to the match data
                for batsman in all_batsmen:
                    self.match_data['batting_stats'][team_key].append({
                        'name': batsman['name'],
                        'runs': batsman['runs'],
                        'balls': batsman['balls'],
                        'fours': batsman['fours'],
                        'sixes': batsman['sixes'],
                        'strike_rate': batsman['strike_rate'],
                        'dismissal': batsman['dismissal'] if batsman['is_out'] else 'not out'
                    })
                    
                    logging.info(f"Batsman: {batsman['name']} - {batsman['runs']} ({batsman['balls']}) - {batsman['dismissal'] if batsman['is_out'] else 'not out'}")
                
                # Only increment if we actually found batsmen
                if self.match_data['batting_stats'][team_key]:
                    batting_count += 1
                
                # Check if innings is complete
                overs = team_data.get('overs', '')
                if overs and '20' in overs:
                    self.match_data['match_info']['innings_status'] = f"{team_key}_complete"
                    logging.info(f"Innings complete for {team_key}")
        
        except Exception as e:
            logging.error(f"Error parsing batting stats: {e}")
    
    def parse_bowling_stats(self, soup):
        """Extract bowling statistics for both teams with enhanced error handling."""
        try:
            # Clear existing bowling stats
            self.match_data['bowling_stats'] = {}
            
            # Initialize empty bowling stats for both teams to ensure we always have the structure
            self.match_data['bowling_stats']['team1'] = []
            self.match_data['bowling_stats']['team2'] = []
            
            # Log the HTML structure to help diagnose issues
            logging.debug("Analyzing HTML for bowling tables")
            
            # Try all possible selectors for finding bowling tables
            possible_selectors = [
                # Exact selectors based on the HTML structure from live pages
                '.ckt_table_card .ckt_bowlers table',
                '.ckt_bowlers table',
                '.ckt_bowlers .b_scard table',
                '.ckt_bowlers .b_scard.b_scardf table',
                # Original selectors
                '.ckt_bowlers, .b_scard table',
                # More generic table selectors that contain bowling data
                'table:contains("BOWLERS")',
                'table:contains("O") table:contains("MO") table:contains("RUNS")',
                'table:contains("O") table:contains("MO") table:contains("WKTS")'
            ]
            
            # Try to find bowling tables using multiple approaches
            bowling_tables = []
            
            # Look in both tabs for bowling data
            for tab_id in ['ckt_fltr_0', 'ckt_fltr_1', 'tab_1']:
                tab_content = None
                
                # Try both data-id and id selectors
                selectors = [f'div[data-id="{tab_id}"]', f'#{tab_id}']
                for selector in selectors:
                    tab_element = soup.select_one(selector)
                    if tab_element:
                        tab_content = tab_element
                        break
                
                if tab_content:
                    # Try each selector within this tab
                    for selector in possible_selectors:
                        tables = tab_content.select(selector)
                        if tables:
                            logging.info(f"Found {len(tables)} potential bowling tables with selector '{selector}' in tab {tab_id}")
                            bowling_tables.extend(tables)
            
            # If not found in tabs, search in the entire document
            if not bowling_tables:
                for selector in possible_selectors:
                    tables = soup.select(selector)
                    if tables:
                        logging.info(f"Found {len(tables)} potential bowling tables with selector '{selector}' in full document")
                        bowling_tables.extend(tables)
            
            if not bowling_tables:
                logging.warning("No bowling tables found with any selectors")
                
                # As a last resort, look for any table that might have bowling data structure
                all_tables = soup.select('table')
                logging.info(f"Found {len(all_tables)} tables in total, checking each for bowling data")
                
                for table in all_tables:
                    table_text = table.text.lower() if table else ''
                    # Check if this might be a bowling table (has headers like Overs, Maidens, etc.)
                    if any(term in table_text for term in ['bowl', 'overs', 'maidens', 'economy']):
                        logging.info("Found potential bowling table by keywords")
                        bowling_tables.append(table)
            
            # Get team information
            team1_data = self.match_data['teams'].get('team1', {})
            team2_data = self.match_data['teams'].get('team2', {})
            
            team1_score = team1_data.get('score', '').lower()
            team2_score = team2_data.get('score', '').lower()
            
            # Log current match phase to help with debugging
            logging.info(f"Team 1 score: {team1_score}")
            logging.info(f"Team 2 score: {team2_score}")
            
            # Determine which team is currently bowling based on match phase
            current_bowling_team = None
            
            if 'yet to bat' in team2_score:
                # Team 1 is batting, Team 2 is bowling
                current_bowling_team = 'team2'
                logging.info("Determined Team 2 is bowling (Team 1 batting, Team 2 yet to bat)")
            elif 'yet to bat' in team1_score:
                # Team 2 is batting, Team 1 is bowling
                current_bowling_team = 'team1'
                logging.info("Determined Team 1 is bowling (Team 2 batting, Team 1 yet to bat)")
            else:
                # Both teams have batted - need to determine current state
                match_status = self.match_data['match_info'].get('status', '').lower()
                logging.info(f"Match status: {match_status}")
                
                # Check for match completion in various ways
                match_completed = (
                    'won by' in match_status or 
                    'match tied' in match_status or 
                    'won the match' in match_status or
                    'match over' in match_status or
                    any(team.get('won') for team in self.match_data['teams'].values())
                )
                
                if match_completed:
                    # Match is complete - process both teams' bowling stats
                    current_bowling_team = None
                    logging.info("Match is complete - will process bowling data for both teams")
                else:
                    # Check which innings we're in based on completed innings
                    team1_innings_complete = team1_data.get('innings_complete', False)
                    team2_innings_complete = team2_data.get('innings_complete', False)
                    
                    logging.info(f"Team 1 innings complete: {team1_innings_complete}")
                    logging.info(f"Team 2 innings complete: {team2_innings_complete}")
                    
                    if team1_innings_complete and not team2_innings_complete:
                        # Team 1 completed innings, now team 2 batting, team 1 bowling
                        current_bowling_team = 'team1'
                        logging.info("Determined Team 1 is bowling (Team 1 innings complete, Team 2 batting)")
                    elif team2_innings_complete and not team1_innings_complete:
                        # Team 2 completed innings, now team 1 batting, team 2 bowling
                        current_bowling_team = 'team2'
                        logging.info("Determined Team 2 is bowling (Team 2 innings complete, Team 1 batting)")
                    else:
                        # Default to matching based on the tab structure
                        # First tab (MI innings) should have CSK bowling stats
                        # Second tab (CSK innings) should have MI bowling stats
                        current_bowling_team = None  # Process both for now
                        logging.info("Processing both teams' bowling data based on tab structure")
            
            # Debug bowling tables
            logging.info(f"Processing {len(bowling_tables)} potential bowling tables")
            
            # Define the header variations for bowling columns
            header_variations = {
                'overs': ['O', 'OVERS', 'OV'],
                'maidens': ['M', 'MO', 'MAIDENS'],
                'runs': ['R', 'RUNS', 'RNS'],
                'wickets': ['W', 'WKTS', 'WICKETS'],
                'economy': ['ECO', 'ECON', 'ECONOMY']
            }
            
            bowling_count = 0
            for i, table in enumerate(bowling_tables):
                try:
                    logging.info(f"Examining table {i+1}")
                    
                    # Debug table content
                    table_text = table.text.strip()
                    logging.debug(f"Table content preview: {table_text[:100]}...")
                    
                    # Check if this is a bowling table by looking for column headers
                    header_row = table.select_one('tr.ckt_row_hdr')
                    header_text = header_row.text if header_row else ''
                    
                    logging.info(f"Header text: {header_text}")
                    
                    # Check for any of the expected bowling header texts
                    is_bowling_table = False
                    
                    if header_row:
                        # Check if headers match bowling table pattern
                        header_cells = header_row.select('td')
                        if len(header_cells) >= 6:
                            header_texts = [cell.get_text(strip=True).upper() for cell in header_cells]
                            
                            # Check if first column says "BOWLERS"
                            if 'BOWLERS' in header_texts[0]:
                                is_bowling_table = True
                            
                            # Check column patterns
                            overs_present = any(o in header_texts for o in header_variations['overs'])
                            wickets_present = any(w in header_texts for w in header_variations['wickets'])
                            runs_present = any(r in header_texts for r in header_variations['runs'])
                            
                            if overs_present and wickets_present and runs_present:
                                is_bowling_table = True
                                logging.info(f"Found bowling table with headers: {header_texts}")
                                
                    if not is_bowling_table:
                        # Try alternative header detection methods
                        all_rows = table.select('tr')
                        if all_rows:
                            first_row = all_rows[0]
                            first_row_text = first_row.text.strip()
                            logging.info(f"First row text: {first_row_text}")
                            
                            if any(kw in first_row_text.upper() for kw in ['BOWLERS', 'BOWLING', 'OVERS', 'ECON']):
                                header_row = first_row
                                is_bowling_table = True
                                logging.info("Found bowling header row using keywords")
                            
                        if not is_bowling_table:
                            logging.info("This doesn't appear to be a bowling table, skipping")
                            continue
                        
                    logging.info("Identified a bowling table")
                    
                    # For first innings, team2 bowls to team1
                    # For second innings, team1 bowls to team2
                    # Determine which team this bowling table belongs to
                    # Strategy: Based on which tab/section it's in
                    
                    # Get the parent elements to determine tab context
                    parent_element = table
                    tab_parent = None
                    
                    # Climb up the tree looking for a tab container
                    for _ in range(5):  # Look up to 5 levels deep
                        if parent_element.parent:
                            parent_element = parent_element.parent
                            parent_id = parent_element.get('id', '')
                            parent_data_id = parent_element.get('data-id', '')
                            
                            if parent_id in ['tab_1', 'tab_2'] or parent_data_id in ['ckt_fltr_0', 'ckt_fltr_1']:
                                tab_parent = parent_id or parent_data_id
                                break
                    
                    # Default assignment based on tab or count
                    if tab_parent:
                        if tab_parent in ['tab_1', 'ckt_fltr_0']:
                            # First tab (MI innings) - has CSK bowling
                            bowling_team = 'team2'
                        else:
                            # Second tab (CSK innings) - has MI bowling
                            bowling_team = 'team1'
                    else:
                        # No tab context found, use count-based assignment
                        bowling_team = 'team2' if bowling_count == 0 else 'team1'
                    
                    logging.info(f"Assigned bowling table to {bowling_team} based on tab context or count")
                    
                    # Skip if we're only processing the current bowling team and this isn't it
                    if current_bowling_team and bowling_team != current_bowling_team:
                        logging.info(f"Skipping table for {bowling_team} as current bowling team is {current_bowling_team}")
                        continue
                    
                    logging.info(f"Processing bowling stats for {bowling_team}")
                    
                    # Get all bowler rows
                    bowler_rows = table.select('tr.ckt_row_item')
                    if not bowler_rows:
                        # Try more generic row selection
                        bowler_rows = table.select('tr')
                        # Skip the header row
                        if bowler_rows and header_row in bowler_rows:
                            bowler_rows.remove(header_row)
                    
                    logging.info(f"Found {len(bowler_rows)} potential bowler rows")
                    
                    for row in bowler_rows:
                        try:
                            # Skip if not a valid bowler row
                            name_cell = row.select_one('td:first-child')
                            if not name_cell:
                                continue
                            
                            # Get player name with improved cleaning
                            player_link = name_cell.select_one('a')
                            player_name = player_link.get_text(strip=True) if player_link else name_cell.get_text(strip=True)
                            
                            # Clean up player name
                            player_name = player_name.replace('\xa0', ' ')  # Replace non-breaking spaces
                            
                            # Fix unclosed parentheses 
                            if '(' in player_name and ')' not in player_name:
                                player_name += ')'
                                
                            # Remove any special characters from beginning
                            player_name = re.sub(r'^\s*[^a-zA-Z]+', '', player_name)
                            
                            # Remove non-alpha chars from end but preserve parenthetical suffixes
                            player_name = re.sub(r'[^a-zA-Z\)]+\s*$', '', player_name)
                            
                            # Skip totals or extras
                            if any(keyword in player_name.lower() for keyword in ['total', 'extras']):
                                continue
                            
                            # Get statistics - handle different column header variations
                            stat_cells = row.select('td')
                            if len(stat_cells) >= 6:
                                # Map the cells to their values, handling header variations
                                # The order is typically:
                                # Player name | Overs | Maidens | Runs | Wickets | Economy
                                
                                # Get text content, handling potential nested HTML elements
                                overs = stat_cells[1].get_text(strip=True)
                                maidens = stat_cells[2].get_text(strip=True)
                                runs = stat_cells[3].get_text(strip=True)
                                wickets = stat_cells[4].get_text(strip=True)
                                economy = stat_cells[5].get_text(strip=True)
                                
                                # Create bowler data
                                bowler_data = {
                                    'name': player_name,
                                    'overs': overs,
                                    'maidens': maidens,
                                    'runs': runs,
                                    'wickets': wickets,
                                    'economy': economy
                                }
                                
                                # Add bowler only if not already in the list
                                self._add_bowler_if_not_exists(bowling_team, bowler_data)
                                
                                logging.info(f"Bowler: {player_name} - {wickets}/{runs} ({overs})")
                        except Exception as row_error:
                            logging.error(f"Error processing bowler row: {row_error}")
                            continue
                    
                    # Only increment if we actually processed this table
                    if self.match_data['bowling_stats'][bowling_team]:
                        bowling_count += 1
                except Exception as table_error:
                    logging.error(f"Error processing bowling table {i+1}: {table_error}")
                    continue
            
            # If we still don't have bowling data, try to infer it
            if not any(self.match_data['bowling_stats'].values()):
                logging.warning("No bowling data found in tables, attempting to infer from match state")
                self._infer_missing_bowling_stats()
            else:
                logging.info("Successfully parsed bowling data")
                
        except Exception as e:
            logging.error(f"Error parsing bowling stats: {e}")
        
        # Log final bowling stats state
        for team, bowlers in self.match_data['bowling_stats'].items():
            logging.info(f"{team} bowling stats: {len(bowlers)} bowlers found")
    
    def _add_bowler_if_not_exists(self, team_key, bowler_data):
        """Add a bowler to the stats only if they don't already exist."""
        # Check if we already have this bowler in our list
        existing_bowlers = {b['name']: i for i, b in enumerate(self.match_data['bowling_stats'][team_key])}
        
        if bowler_data['name'] in existing_bowlers:
            # Replace existing entry if it's the same bowler (newer data might be more accurate)
            self.match_data['bowling_stats'][team_key][existing_bowlers[bowler_data['name']]] = bowler_data
            logging.debug(f"Updated existing bowler: {bowler_data['name']}")
        else:
            # Add new bowler
            self.match_data['bowling_stats'][team_key].append(bowler_data)
            logging.info(f"Added new bowler: {bowler_data['name']}")
    
    def _infer_missing_bowling_stats(self):
        """Attempt to infer bowling statistics when they can't be parsed from HTML."""
        try:
            # If Team 1 has batted, Team 2 must have bowled to them
            team1_data = self.match_data['teams'].get('team1', {})
            team1_score = team1_data.get('score', '').lower()
            
            if team1_score and 'yet to bat' not in team1_score:
                # Get batsmen for team1
                team1_batsmen = self.match_data['batting_stats'].get('team1', [])
                
                if team1_batsmen:
                    # Create placeholder bowling stats for team2
                    self.match_data['bowling_stats']['team2'] = [{
                        'name': 'Bowling data not available',
                        'overs': team1_data.get('overs', '0.0'),
                        'maidens': '0',
                        'runs': team1_data.get('runs', '0'),
                        'wickets': team1_data.get('wickets', '0'),
                        'economy': '0.00',
                        'inferred': True
                    }]
                    logging.info("Inferred placeholder bowling stats for team2")
            
            # If Team 2 has batted, Team 1 must have bowled to them
            team2_data = self.match_data['teams'].get('team2', {})
            team2_score = team2_data.get('score', '').lower()
            
            if team2_score and 'yet to bat' not in team2_score:
                # Get batsmen for team2
                team2_batsmen = self.match_data['batting_stats'].get('team2', [])
                
                if team2_batsmen:
                    # Create placeholder bowling stats for team1
                    self.match_data['bowling_stats']['team1'] = [{
                        'name': 'Bowling data not available',
                        'overs': team2_data.get('overs', '0.0'),
                        'maidens': '0',
                        'runs': team2_data.get('runs', '0'),
                        'wickets': team2_data.get('wickets', '0'),
                        'economy': '0.00',
                        'inferred': True
                    }]
                    logging.info("Inferred placeholder bowling stats for team1")
        
        except Exception as e:
            logging.error(f"Error inferring bowling stats: {e}")
    
    def parse_commentary(self, soup):
        """Extract the latest commentary updates."""
        try:
            # Look for commentary tab or section
            commentary_section = soup.select_one('#tab_2, .ckt_gamecomm')
            
            # If not found in dedicated tab, look in any tab
            if not commentary_section:
                commentary_section = soup
            
            # Find commentary items
            commentary_items = commentary_section.select('.ckt_commentary_item')
            
            if not commentary_items:
                # Try alternative selectors
                commentary_items = soup.select('.ckt_comm_time, .ckt_comm_ball')
            
            if not commentary_items:
                logging.warning("No commentary items found")
                return
            
            # Reset commentary list
            self.match_data['commentary'] = []
            
            # Process commentary items
            for item in commentary_items[:20]:
                # General commentary (with timestamp)
                time_elem = item.select_one('.ckt_comm_time')
                
                if time_elem:
                    # Get time from the bold element
                    time_bold = time_elem.select_one('b')
                    time_text = time_bold.text.strip() if time_bold else ''
                    
                    # Get commentary text (excluding time)
                    text = ""
                    for child in time_elem.contents:
                        if child.name != 'b':
                            text += str(child).strip()
                    
                    text = re.sub(r'^\s*', '', text)
                    
                    self.match_data['commentary'].append({
                        'type': 'general',
                        'time': time_text,
                        'text': text
                    })
                    continue
                
                # Ball-by-ball commentary
                ball_elem = item.select_one('.ckt_comm_ball')
                if ball_elem:
                    over_elem = ball_elem.select_one('.ckt_overs')
                    result_elem = ball_elem.select_one('.ckt_ball')
                    
                    over = over_elem.text.strip() if over_elem else ''
                    result = result_elem.text.strip() if result_elem else ''
                    
                    # Get commentary text
                    text_elem = item.select_one('.ckt_comm_txt')
                    text = text_elem.text.strip() if text_elem else ''
                    
                    self.match_data['commentary'].append({
                        'type': 'ball',
                        'over': over,
                        'result': result,
                        'text': text
                    })
            
            logging.info(f"Extracted {len(self.match_data['commentary'])} commentary items")
        
        except Exception as e:
            logging.error(f"Error parsing commentary: {e}")
    
    def parse_html(self, html_content):
        """Parse the HTML content to extract match data."""
        if not html_content:
            logging.error("No HTML content to parse")
            return
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Parse different sections of the match data
            self.parse_match_info(soup)
            self.parse_teams_and_scores(soup)
            self.parse_batting_stats(soup)
            self.parse_bowling_stats(soup)
            self.parse_commentary(soup)
        
        except Exception as e:
            logging.error(f"Error parsing HTML: {e}")
    
    def update(self):
        """Fetch the latest data and update the match information."""
        html_content = self.fetch_data()
        if not html_content:
            logging.warning("Failed to fetch content. Retrying in next update...")
            return
        
        # Save raw HTML for debugging
        self.save_debug_html(html_content)
        
        # Parse the HTML content
        self.parse_html(html_content)
        
        # Update timestamp
        self.match_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Validate and clean up the data
        self.validate_data()
        
        return self.match_data
        
    def validate_data(self):
        """Validate the parsed data for common issues and fix them."""
        
        logging.info("Validating parsed data...")
        
        # 1. Check if we're missing batting stats for any team that has batted
        for team_key, team_data in self.match_data['teams'].items():
            score = team_data.get('score', '').lower()
            if score and 'yet to bat' not in score:
                # Team has batted but might be missing from batting_stats
                if team_key not in self.match_data['batting_stats'] or not self.match_data['batting_stats'][team_key]:
                    logging.warning(f"{team_key} ({team_data.get('name', '')}) has batted but no batting stats found!")
                    
                    # Try to find the other team's bowling data to infer this team batted
                    other_team = 'team2' if team_key == 'team1' else 'team1'
                    if other_team in self.match_data['bowling_stats'] and self.match_data['bowling_stats'][other_team]:
                        logging.info(f"Found bowling data for {other_team}, which confirms {team_key} has batted")
        
        # 2. Check for duplicate bowlers in bowling stats
        for team_key, bowlers in self.match_data['bowling_stats'].items():
            unique_bowlers = {}
            duplicate_count = 0
            
            for i, bowler in enumerate(bowlers):
                name = bowler['name']
                if name in unique_bowlers:
                    duplicate_count += 1
                else:
                    unique_bowlers[name] = i
            
            if duplicate_count > 0:
                logging.warning(f"Found {duplicate_count} duplicate bowlers in {team_key}")
                
                # Fix by keeping only unique bowlers
                self.match_data['bowling_stats'][team_key] = [bowlers[i] for i in unique_bowlers.values()]
                logging.info(f"Removed duplicates, now {team_key} has {len(self.match_data['bowling_stats'][team_key])} bowlers")
        
        # 3. Check for players with incomplete names
        for team_key, batsmen in self.match_data['batting_stats'].items():
            for i, batsman in enumerate(batsmen):
                name = batsman['name']
                if '(' in name and ')' not in name:
                    # Fix incomplete parenthesis
                    fixed_name = name + ')'
                    logging.info(f"Fixed incomplete name: {name} -> {fixed_name}")
                    self.match_data['batting_stats'][team_key][i]['name'] = fixed_name
        
        logging.info("Data validation complete")
    
    def save_match_data(self):
        """Save the current match data to a timestamped JSON file."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Try to get team names for the filename
            team1 = self.match_data['teams'].get('team1', {}).get('name', 'unknown')
            team2 = self.match_data['teams'].get('team2', {}).get('name', 'unknown')
            
            # Clean the team names for the filename
            team1 = re.sub(r'[^a-zA-Z0-9]', '_', team1)
            team2 = re.sub(r'[^a-zA-Z0-9]', '_', team2)
            
            filename = f"{self.log_dir}/{team1}_vs_{team2}_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.match_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Match data saved to {filename}")
            return filename
        except Exception as e:
            logging.error(f"Error saving match data: {e}")
            return None