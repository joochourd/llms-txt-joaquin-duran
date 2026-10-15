package com.joaquinduran.llmstxt.crawl.analysis

/** Text thresholds count cleaned characters; extraction caps bound retained evidence. Values are pipeline calibration. */
internal object ContentThresholds {
    // Short pages can also qualify through a useful title and description; this is not a universal minimum page size.
    const val MIN_USEFUL_TEXT_LENGTH = 80
    const val MIN_SHORT_TEXT_LENGTH = 50
    const val SUBSTANTIAL_TEXT_LENGTH = 200
    const val LONG_TEXT_LENGTH = 1000
    const val MIN_USEFUL_TITLE_LENGTH = 8
    const val MIN_USEFUL_DESCRIPTION_LENGTH = 40
    const val CHALLENGE_TEXT_LIMIT = 2000
    const val MIN_SEED_CHILDREN = 2
    const val MIN_HUB_CHILDREN = 2
    const val HTML_REQUEST_RESERVE = 2
    const val MAX_ANCHOR_LENGTH = 240
    const val MAX_GROUP_LENGTH = 80
    const val MAX_STRUCTURED_DATA_LENGTH = 8192
    const val MAX_HEADINGS = 30
    const val MAX_HEADING_LENGTH = 120
}

/** Evidence points are relative signals, not probabilities. The margin avoids assigning an ambiguous context. */
internal object ContextWeights {
    const val STRUCTURAL_SIGNAL = 3
    const val EXPLICIT_SIGNAL = 2
    const val SUPPORTING_SIGNAL = 1
    const val MIN_RELATED_LINKS = 3
    const val MIN_CONTEXT_SCORE = 4
    const val MIN_CONTEXT_MARGIN = 2
    const val HIGH_CONFIDENCE_SCORE = 7
}

/** Relative relevance points. Version and evaluate changes against the corpus instead of tuning them per request. */
internal object RankingWeights {
    const val NAVIGATION = 20.0
    const val ARTICLE_NAVIGATION = 5.0
    const val SECONDARY_NAVIGATION = 10.0
    const val MAIN_CONTENT = 15.0
    const val ARTICLE_MAIN_CONTENT = 30.0
    const val FOOTER = -20.0
    const val SITEMAP = 5.0
    const val SEED_AFFINITY = 25.0
    const val PARENT_PATH_AFFINITY = 15.0
    const val SHARED_PATH_AFFINITY = 8.0
    const val DOCUMENTATION = 15.0
    const val PRODUCT = 15.0
    const val CONTEXT_BONUS = 10
    const val TOPIC = 10.0
    const val COMMERCE_POLICY = 20.0
    const val MAX_SIMPLE_QUERY_PARAMETERS = 2
    const val COMPLEX_QUERY = -15.0
    const val LONG_CONTENT = 15.0
    const val SUBSTANTIAL_CONTENT = 8.0
    const val USEFUL_DESCRIPTION = 5.0
    const val USEFUL_TITLE = 5.0
    const val NOINDEX = -10.0
    const val SECTION_OVERVIEW = 10
    const val FAMILY_REPETITION_PENALTY = 12.0
}
