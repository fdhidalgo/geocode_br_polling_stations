#' Conservative Blocking Strategy for 100% Recall
#' 
#' Alternative blocking function that prioritizes recall over reduction
#' to ensure no true matches are missed
#'
#' @import data.table
#' @import stringr

#' Extract word fragments for conservative matching
#' 
#' Extracts both full words and word fragments to catch variations
#' 
#' @param text Character vector of text to process
#' @param min_fragment_length Minimum fragment length (default: 4)
#' @return List of character vectors with words and fragments
#' @export
# Note: 2 unused functions were moved to backup/unused_functions/
# Date: 2025-06-20
# Functions removed: create_conservative_blocked_pairs, recommend_blocking_mode

extract_words_and_fragments <- function(text, min_fragment_length = 4) {
  # Handle NA and empty strings
  if (length(text) == 0) return(list())
  
  # Convert to uppercase for consistent matching
  text <- toupper(as.character(text))
  text[is.na(text)] <- ""
  
  # Extract significant words
  words <- extract_significant_words(text)
  
  # For each text, also extract word fragments
  fragments_list <- lapply(seq_along(text), function(i) {
    if (length(words[[i]]) == 0) return(character(0))
    
    # Get fragments from each word
    fragments <- unlist(lapply(words[[i]], function(word) {
      if (nchar(word) <= min_fragment_length) return(word)
      
      # Extract beginning and end fragments
      frags <- c(
        substr(word, 1, min_fragment_length),  # First N chars
        substr(word, nchar(word) - min_fragment_length + 1, nchar(word))  # Last N chars
      )
      unique(frags)
    }))
    
    # Combine full words and fragments
    unique(c(words[[i]], fragments))
  })
  
  fragments_list
}
