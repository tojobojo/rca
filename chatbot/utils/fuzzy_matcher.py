# utils/fuzzy_matcher.py - Fuzzy string matching for pipeline names
from typing import List, Tuple, Optional
import logging

logger = logging.getLogger("rca_bot.fuzzy_matcher")


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings.
    
    Args:
        s1: First string
        s2: Second string
    
    Returns:
        Edit distance between the strings
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost of insertions, deletions, or substitutions
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate similarity score between two strings (0.0 to 1.0).
    
    Uses normalized Levenshtein distance.
    
    Args:
        str1: First string
        str2: Second string
    
    Returns:
        Similarity score (1.0 = identical, 0.0 = completely different)
    """
    str1_lower = str1.lower()
    str2_lower = str2.lower()
    
    if str1_lower == str2_lower:
        return 1.0
    
    max_len = max(len(str1_lower), len(str2_lower))
    if max_len == 0:
        return 1.0
    
    distance = levenshtein_distance(str1_lower, str2_lower)
    similarity = 1.0 - (distance / max_len)
    
    return similarity


def partial_match_score(partial: str, full: str) -> float:
    """
    Calculate score for partial string matching.
    
    Gives higher scores when the partial string appears at the beginning
    or as a complete word in the full string.
    
    Args:
        partial: Partial string (e.g., "merchant")
        full: Full string (e.g., "merchant_peer_grouping")
    
    Returns:
        Match score (0.0 to 1.0)
    """
    partial_lower = partial.lower()
    full_lower = full.lower()
    
    # Exact match
    if partial_lower == full_lower:
        return 1.0
    
    # Full string contains partial
    if partial_lower in full_lower:
        # Bonus if it's at the start
        if full_lower.startswith(partial_lower):
            return 0.95
        
        # Check if it's a word boundary match
        words = full_lower.replace('_', ' ').replace('-', ' ').split()
        for word in words:
            if word.startswith(partial_lower):
                return 0.9
            if partial_lower in word:
                return 0.8
        
        # General substring match
        return 0.7
    
    # Use similarity for fuzzy matching
    return calculate_similarity(partial_lower, full_lower)


def find_best_matches(
    user_input: str,
    candidates: List[str],
    threshold: float = 0.6,
    max_results: int = 5
) -> List[Tuple[str, float, str]]:
    """
    Find best matching candidates for user input.
    
    Args:
        user_input: User's input string
        candidates: List of candidate strings to match against
        threshold: Minimum similarity threshold (0.0 to 1.0)
        max_results: Maximum number of results to return
    
    Returns:
        List of tuples: (candidate, score, match_type)
        match_type can be: "exact", "partial", "fuzzy"
    """
    if not user_input or not candidates:
        return []
    
    user_input_lower = user_input.lower().strip()
    matches = []
    
    for candidate in candidates:
        # Check for exact match
        if user_input_lower == candidate.lower():
            matches.append((candidate, 1.0, "exact"))
            continue
        
        # Check for partial match
        partial_score = partial_match_score(user_input, candidate)
        if partial_score >= 0.7:
            matches.append((candidate, partial_score, "partial"))
            continue
        
        # Fuzzy match using similarity
        similarity = calculate_similarity(user_input, candidate)
        if similarity >= threshold:
            matches.append((candidate, similarity, "fuzzy"))
    
    # Sort by score (descending)
    matches.sort(key=lambda x: x[1], reverse=True)
    
    # Return top results
    result = matches[:max_results]
    
    if result:
        logger.info(f"Fuzzy matching '{user_input}': found {len(result)} matches")
        for candidate, score, match_type in result[:3]:
            logger.debug(f"  - {candidate}: {score:.2f} ({match_type})")
    else:
        logger.info(f"Fuzzy matching '{user_input}': no matches above threshold {threshold}")
    
    return result


def get_best_match(
    user_input: str,
    candidates: List[str],
    threshold: float = 0.6
) -> Optional[Tuple[str, float, str]]:
    """
    Get single best match for user input.
    
    Args:
        user_input: User's input string
        candidates: List of candidate strings to match against
        threshold: Minimum similarity threshold
    
    Returns:
        Tuple of (candidate, score, match_type) or None if no match
    """
    matches = find_best_matches(user_input, candidates, threshold, max_results=1)
    return matches[0] if matches else None


def should_auto_confirm(score: float, match_type: str) -> bool:
    """
    Determine if a match should be auto-confirmed without asking user.
    
    Args:
        score: Match score (0.0 to 1.0)
        match_type: Type of match ("exact", "partial", "fuzzy")
    
    Returns:
        True if should auto-confirm, False if should ask for confirmation
    """
    # Always auto-confirm exact matches
    if match_type == "exact":
        return True
    
    # Auto-confirm high-confidence partial matches
    if match_type == "partial" and score >= 0.9:
        return True
    
    # Auto-confirm very high similarity fuzzy matches
    if score >= 0.85:
        return True
    
    return False


def needs_confirmation(score: float, match_type: str) -> bool:
    """
    Determine if a match needs user confirmation.
    
    Args:
        score: Match score (0.0 to 1.0)
        match_type: Type of match
    
    Returns:
        True if needs confirmation, False otherwise
    """
    return not should_auto_confirm(score, match_type)
