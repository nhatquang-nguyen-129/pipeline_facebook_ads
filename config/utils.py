"""
===================================================================
FACEBOOK UTILITIES MODULE
-------------------------------------------------------------------
This module provides **utility functions** to support the  
Facebook Ads data pipeline, focusing on reusable and generic  
helpers that keep the main ETL code clean and maintainable.

It acts as a shared toolkit used across different components  
of the pipeline, reducing duplication and centralizing  
common logic.

✔️ Handles environment variable parsing and secrets retrieval  
✔️ Provides string manipulation and formatting helpers  
✔️ Supports error handling and logging utilities  

⚠️ This module does *not* define schema or ETL process logic.  
It only contains supportive utilities to simplify and  
standardize code across the pipeline.
===================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python Pandas library for data processing
import pandas as pd

# Add Python "re" library for expression matching
import re

# 1. STRING PROCESSING ULTILITIES

# 1.1. Remove accents from unicode strings
def remove_string_accents(text: str) -> str:
    if not isinstance(text, str):
        return text
    
    # 1.1.1. Define mapping for lowercase unicode characters
    vietnamese_map = {
        'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
        'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
        'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',

        'đ': 'd',

        'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
        'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',

        'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',

        'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
        'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
        'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',

        'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
        'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',

        'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
    }
    
    # 1.1.2. Generate mapping for uppercase characters
    vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map.items()}

    # 1.1.3. Combine lower and upper mappings
    full_map = {**vietnamese_map, **vietnamese_map_upper}
    return ''.join(full_map.get(c, c) for c in text)