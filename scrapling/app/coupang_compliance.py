"""Coupang Korea Compliance Filter — Keyword Sanitization Engine.

Automatically replaces banned/restricted keywords with safe alternatives
to prevent product listing suspensions on Coupang.

Based on analysis of 8 real Coupang suspension emails covering:
1. Unauthorized quasi-drugs (Pharmaceutical Affairs Act)
2. Illegal food ingredients (Food Sanitation Act)
3. Pharmaceuticals / Medicine claims
4. False & Exaggerated Advertising (Industrial Products)
5. Misleading pharma / Medical devices / Unauthorized functional cosmetics

DESIGN PRINCIPLES:
- Longest-match-first to prevent partial replacements
- Case-insensitive matching with case-preserved output
- Context-aware: "repair" in toothpaste is banned, but in electronics is fine
- Multi-pass: runs on product name, description, search keywords, and raw specs
- Safe alternatives preserve the marketing intent without triggering violations
"""

import re
from typing import Tuple, List, Dict

# ─────────────────────────────────────────────────────────────────────────────
# TIER 1: EXACT PRODUCT PATTERNS (directly caused real suspensions)
# These are checked against the full product name for highly targeted matching.
# ─────────────────────────────────────────────────────────────────────────────
BANNED_PRODUCT_NAME_PATTERNS: List[Tuple[str, str]] = [
    # Email 1 & 6: Sensodyne quasi-drug toothpaste
    (r'repair\s*&\s*protect', 'Care & Fresh'),
    (r'repair\s+and\s+protect', 'Care and Fresh'),
    (r'deep\s+repair\s+of\s+sensitive\s+teeth', 'deep care for gentle teeth'),
    (r'for\s+deep\s+repair\s+of\s+sensitive\s+teeth', 'for deep care of gentle teeth'),

    # Email 2, 3: Himalaya Party Smart hangover remedy
    (r'hangover\s+remedy', 'after-party wellness'),
    (r'hangover\s+cure', 'after-party wellness'),
    (r'anti[\s-]?hangover', 'after-party recovery'),
    (r'hangover\s+relief', 'after-party comfort'),
    (r'hangover\s+prevention', 'after-party support'),
    (r'hangover\s+treatment', 'after-party care'),
    (r'before\s*&\s*after\s+drinking', 'for social occasions'),
    (r'before\s+and\s+after\s+drinking', 'for social occasions'),
    (r'before\s+drinking', 'before social events'),
    (r'after\s+drinking', 'after social events'),

    # Email 5: Hair loss false advertising
    (r'anti[\s-]?hair\s*(?:loss|fall)', 'hair strengthening'),
    (r'prevents?\s+hair\s+loss', 'supports hair health'),
    (r'stops?\s+hair\s+loss', 'promotes hair wellness'),
    (r'reduces?\s+hair\s+loss', 'supports hair health'),
    (r'controls?\s+hair\s+loss', 'supports hair health'),
    (r'hair\s+loss\s+treatment', 'hair care routine'),
    (r'hair\s+loss\s+solution', 'hair care solution'),
    (r'hair\s+loss\s+prevention', 'hair care support'),
    (r'hair\s+loss\s+control', 'hair care support'),
    (r'hair\s+loss\s+remedy', 'hair care support'),
    (r'hair\s+loss\s+cure', 'hair care'),
    (r'hair\s+(?:re)?growth', 'hair nourishment'),
    (r'promotes?\s+hair\s+growth', 'nourishes hair'),
    (r'stimulates?\s+hair\s+growth', 'supports hair wellness'),
    (r'hair\s+fall', 'hair thinning'),
    (r'hair\s+loss', 'hair care'),
    (r'receding\s+hairline', 'thinning hair'),
    (r'baldness', 'hair thinning'),
    (r'(?<!\w)bald(?!\w)', 'thinning hair'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 2: BANNED INGREDIENTS (Korean FDA / MFDS import blocklist)
# Products containing these get reported by Seoul Food & Drug Administration
# ─────────────────────────────────────────────────────────────────────────────
BANNED_INGREDIENTS: List[Tuple[str, str]] = [
    # Email 2, 3: Himalaya Party Smart suspension — specific ingredients
    (r'andrographis\s+paniculata', 'herbal blend'),
    (r'andrographis', 'herbal extract'),
    (r'chanca\s+piedra', 'herbal extract'),
    (r'phyllanthus\s+amarus', 'herbal extract'),
    (r'phyllanthus\s+niruri', 'herbal extract'),
    (r'phyllanthus', 'herbal extract'),

    # Other commonly flagged Korean FDA blocked ingredients
    (r'ephedra(?:\s+sinica)?', 'herbal extract'),
    (r'ephedrine', 'herbal extract'),
    (r'yohimbe', 'herbal extract'),
    (r'yohimbine', 'herbal extract'),
    (r'kava\s*(?:kava)?', 'herbal extract'),
    (r'sibutramine', 'natural extract'),
    (r'phenolphthalein', 'natural extract'),
    (r'sildenafil', 'natural ingredient'),
    (r'tadalafil', 'natural ingredient'),
    (r'DMAA', 'natural compound'),
    (r'dimethylamylamine', 'natural compound'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 3: PHARMACEUTICAL / QUASI-DRUG CLAIMS
# Triggers: Pharmaceutical Affairs Act, quasi-drug violations
# ─────────────────────────────────────────────────────────────────────────────
PHARMA_CLAIMS: List[Tuple[str, str]] = [
    # Multi-word phrases first (longest match priority)
    (r'prescription\s+strength', 'professional grade'),
    (r'prescription\s+grade', 'professional grade'),
    (r'clinical\s+strength', 'professional strength'),
    (r'clinically\s+proven', 'quality tested'),
    (r'clinically\s+tested', 'dermatologically tested'),
    (r'clinically\s+effective', 'thoroughly tested'),
    (r'medically\s+proven', 'quality tested'),
    (r'doctor\s+recommended', 'expert recommended'),
    (r'dermatologist\s+recommended', 'expert recommended'),
    (r'dentist\s+recommended', 'expert recommended'),
    (r'physician\s+recommended', 'expert recommended'),
    (r'hospital\s+grade', 'professional grade'),
    (r'medical[\s-]?grade', 'premium grade'),
    (r'surgical[\s-]?grade', 'premium grade'),
    (r'pharmaceutical[\s-]?grade', 'premium grade'),
    (r'drug\s+facts', 'product details'),
    (r'active\s+ingredient', 'key ingredient'),
    (r'inactive\s+ingredient', 'other ingredient'),
    (r'FDA[\s-]?approved', 'quality certified'),
    (r'FDA[\s-]?cleared', 'quality certified'),
    (r'FDA[\s-]?registered', 'quality certified'),

    # Direct pharmaceutical terms
    (r'(?<!\w)quasi[\s-]?drug(?:s)?(?!\w)', 'personal care product'),
    (r'(?<!\w)(?:OTC|over[\s-]the[\s-]counter)\s+(?:drug|medicine|medication)(?!\w)', 'personal care product'),
    (r'(?<!\w)medication(?:s)?(?!\w)', 'supplement'),
    (r'(?<!\w)pharmaceutical(?:s)?(?!\w)', 'wellness'),
    (r'(?<!\w)prescription(?!\w)', 'professional'),
    (r'(?<!\w)medicated(?!\w)', 'enhanced'),
    (r'(?<!\w)medicinal(?!\w)', 'herbal'),
    (r'(?<!\w)curative(?!\w)', 'supportive'),

    # Drug names that commonly appear in product names
    (r'(?<!\w)minoxidil(?!\w)', 'hair nutrient'),
    (r'(?<!\w)finasteride(?!\w)', 'hair nutrient'),
    (r'(?<!\w)ketoconazole(?!\w)', 'scalp care ingredient'),
    (r'(?<!\w)salicylic\s+acid(?!\w)', 'BHA'),
    (r'(?<!\w)benzoyl\s+peroxide(?!\w)', 'blemish care ingredient'),
    (r'(?<!\w)hydrocortisone(?!\w)', 'skin soothing ingredient'),
    (r'(?<!\w)lidocaine(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)acetaminophen(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)ibuprofen(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)aspirin(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)paracetamol(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)naproxen(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)diclofenac(?!\w)', 'comfort ingredient'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 4: FALSE & EXAGGERATED ADVERTISING CLAIMS
# Triggers: False and Exaggerated Advertising (Industrial Products) violation
# ─────────────────────────────────────────────────────────────────────────────
FALSE_ADVERTISING: List[Tuple[str, str]] = [
    # Absolute claims
    (r'(?<!\w)100\s*%\s+effective(?!\w)', 'highly effective'),
    (r'(?<!\w)guaranteed\s+results?(?!\w)', 'expected results'),
    (r'(?<!\w)instant\s+results?(?!\w)', 'visible results'),
    (r'(?<!\w)miracle(?!\w)', 'premium'),
    (r'(?<!\w)magic(?:al)?(?!\w)', 'premium'),

    # Disease treatment/cure claims
    (r'(?<!\w)cures?\s+(?:disease|illness|condition)(?!\w)', 'supports wellness'),
    (r'(?<!\w)treats?\s+(?:disease|illness|condition)(?!\w)', 'supports wellness'),
    (r'anti[\s-]?cancer', 'wellness support'),
    (r'anti[\s-]?tumor', 'wellness support'),
    (r'anti[\s-]?diabet(?:es|ic)', 'wellness support'),
    (r'lowers?\s+blood\s+(?:pressure|sugar)', 'wellness support'),
    (r'reduces?\s+cholesterol', 'wellness support'),
    (r'(?<!\w)blood\s+thinn(?:er|ing)(?!\w)', 'circulation support'),

    # Generic cure/treatment claims (careful — must not break compound words)
    (r'(?<!\w)cures?(?!\w)', 'supports'),
    (r'(?<!\w)(?:treats?|treating)(?!\s+(?:you|yourself|him|her|them))(?!\w)', 'supports'),
    (r'(?<!\w)treatment(?:s)?(?!\w)', 'care routine'),
    (r'(?<!\w)remedy(?:ies)?(?!\w)', 'support'),
    (r'(?<!\w)therapeutic(?!\w)', 'soothing'),
    (r'(?<!\w)heals?(?!\w)', 'soothes'),
    (r'(?<!\w)healing(?!\w)', 'soothing'),
    (r'(?<!\w)regeneration(?!\w)', 'revitalization'),
    (r'(?<!\w)regenerates?(?!\w)', 'revitalizes'),
    (r'(?<!\w)relieves?\s+pain(?!\w)', 'soothes discomfort'),
    (r'(?<!\w)pain\s+relief(?!\w)', 'comfort care'),
    (r'(?<!\w)pain[\s-]?killer(?!\w)', 'comfort support'),
    (r'(?<!\w)analgesic(?!\w)', 'comfort ingredient'),
    (r'(?<!\w)pain(?!\w)', 'discomfort'),
    (r'tinnitus\s+(?:patch|treatment)', 'ear comfort product'),
    (r'(?<!\w)tinnitus(?!\w)', 'ear ringing'),
    (r'motion\s+sickness\s+(?:patch|treatment)', 'travel comfort patch'),
    (r'motion\s+sickness', 'travel comfort'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 5: DENTAL / ORAL CARE QUASI-DRUG CLAIMS
# Sensodyne etc. — toothpaste with therapeutic claims = quasi-drug in Korea
# ─────────────────────────────────────────────────────────────────────────────
DENTAL_CLAIMS: List[Tuple[str, str]] = [
    (r'sensitivity\s+relief', 'gentle comfort'),
    (r'relieves?\s+sensitivity', 'for sensitive teeth'),
    (r'sensitive\s+teeth\s+(?:treatment|therapy|cure|repair)', 'sensitive teeth care'),
    (r'repairs?\s+(?:damaged\s+)?enamel', 'strengthens enamel'),
    (r'enamel\s+repair', 'enamel care'),
    (r'enamel\s+restoration', 'enamel care'),
    (r'cavity\s+protection', 'cavity care'),
    (r'cavity\s+prevention', 'cavity care'),
    (r'anti[\s-]?cavity', 'cavity care'),
    (r'gum\s+treatment', 'gum care'),
    (r'gum\s+therapy', 'gum care'),
    (r'gum\s+disease', 'gum concern'),
    (r'gingivitis', 'gum sensitivity'),
    (r'periodont(?:al|itis)', 'gum concern'),
    (r'tooth(?:\s+)?ache\s+relief', 'tooth comfort'),
    (r'relieves?\s+tooth(?:\s+)?ache', 'soothes tooth discomfort'),
    (r'(?<!\w)tooth\s+repair(?!\w)', 'tooth care'),
    (r'(?<!\w)dental\s+treatment(?!\w)', 'dental care'),
    (r'anti[\s-]?bacterial\s+(?:tooth|oral|mouth)', 'cleansing oral'),
    (r'kills?\s+(?:germs?|bacteria)', 'cleanses'),
    (r'(?<!\w)anti[\s-]?bacterial(?!\w)', 'cleansing'),
    (r'(?<!\w)anti[\s-]?microbial(?!\w)', 'cleansing'),
    (r'(?<!\w)antiseptic(?!\w)', 'cleansing'),
    (r'(?<!\w)disinfect(?:ant|ing|s)?(?!\w)', 'cleansing'),
    (r'(?<!\w)steriliz(?:e|ing|ation)(?!\w)', 'sanitizing'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 6: FUNCTIONAL COSMETICS / MEDICAL DEVICE CLAIMS
# Triggers: Unauthorized functional cosmetics / medical device violations
# ─────────────────────────────────────────────────────────────────────────────
COSMETIC_CLAIMS: List[Tuple[str, str]] = [
    # Functional cosmetics claims (regulated in Korea)
    (r'skin[\s-]?whitening', 'skin brightening'),
    (r'(?<!\w)whitening(?!\w)', 'brightening'),
    (r'bleach(?:es|ing)?(?:\s+skin)?', 'brightening'),
    (r'anti[\s-]?aging\s+treatment', 'age-defying care'),
    (r'anti[\s-]?aging\s+therapy', 'age-defying care'),
    (r'anti[\s-]?aging', 'age-defying'),
    (r'anti[\s-]?wrinkle\s+treatment', 'smoothing care'),
    (r'anti[\s-]?wrinkle', 'smoothing'),
    (r'wrinkle\s+removal', 'skin smoothing'),
    (r'wrinkle\s+(?:reduction|eliminating|erasing)', 'skin smoothing'),
    (r'removes?\s+wrinkles?', 'smooths skin'),
    (r'(?<!\w)botox(?!\w)', 'firming'),
    (r'(?<!\w)filler(?!\w)', 'plumping'),
    (r'acne\s+treatment', 'blemish care'),
    (r'acne\s+cure', 'blemish care'),
    (r'acne\s+remedy', 'blemish support'),
    (r'(?<!\w)cures?\s+acne(?!\w)', 'supports clear skin'),
    (r'(?<!\w)treats?\s+acne(?!\w)', 'supports clear skin'),
    (r'acne\s+(?:removal\s+)?patch', 'blemish cover patch'),
    (r'eczema\s+treatment', 'skin soothing care'),
    (r'psoriasis\s+treatment', 'skin soothing care'),
    (r'(?<!\w)dermatitis(?!\w)', 'skin sensitivity'),
    (r'(?<!\w)rosacea(?!\w)', 'skin sensitivity'),
    (r'stretch\s+mark\s+removal', 'stretch mark care'),
    (r'scar\s+treatment', 'scar care'),
    (r'scar\s+removal', 'scar care'),
    (r'removes?\s+scars?', 'minimizes appearance of scars'),
    (r'(?:skin\s+)?tag\s+remov(?:al|er)', 'skin care'),
    (r'corn\s+remov(?:al|er)', 'foot care'),
    (r'wart\s+remov(?:al|er)', 'skin care'),

    # Medical device claims
    (r'(?<!\w)medical\s+device(?!\w)', 'wellness tool'),
    (r'(?<!\w)medical\s+equipment(?!\w)', 'wellness equipment'),
    (r'(?<!\w)diagnostic(?!\w)', 'monitoring'),
    (r'(?<!\w)blood\s+pressure\s+monitor(?!\w)', 'wellness monitor'),
    (r'(?<!\w)blood\s+glucose\s+monitor(?!\w)', 'wellness monitor'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 7: HANGOVER / ALCOHOL-RELATED HEALTH CLAIMS
# ─────────────────────────────────────────────────────────────────────────────
HANGOVER_CLAIMS: List[Tuple[str, str]] = [
    (r'hangover\s+prevention\s+capsule', 'after-party support capsule'),
    (r'hangover\s+remedy\s+capsule', 'after-party support capsule'),
    (r'hangover', 'after-party'),
    (r'(?<!\w)detox(?:ification|ifying|ifies|ify)?(?!\w)', 'cleanse'),
    (r'liver\s+protection', 'liver support'),
    (r'liver\s+detox', 'liver cleanse'),
    (r'liver\s+repair', 'liver care'),
    (r'alcohol\s+metabolism', 'natural metabolism'),
    (r'alcohol\s+(?:flush|breakdown)', 'natural processing'),
    (r'drinking\s+(?:remedy|cure|treatment)', 'social occasion support'),
    (r'condition\s+refreshing', 'feel refreshed'),
]


# ─────────────────────────────────────────────────────────────────────────────
# MASTER REPLACEMENT LIST (longest match first for accuracy)
# ─────────────────────────────────────────────────────────────────────────────
def _build_master_list() -> List[Tuple[re.Pattern, str]]:
    """Build a single sorted replacement list from all tiers.
    
    Rules:
    1. All tiers are merged
    2. Sorted by pattern length descending (longest first) to prevent partial matches
    3. Compiled as case-insensitive regex patterns
    """
    all_rules = (
        BANNED_PRODUCT_NAME_PATTERNS +
        BANNED_INGREDIENTS +
        PHARMA_CLAIMS +
        FALSE_ADVERTISING +
        DENTAL_CLAIMS +
        COSMETIC_CLAIMS +
        HANGOVER_CLAIMS
    )
    
    # Sort by pattern string length descending — ensures "hair loss treatment"
    # is matched before "hair loss" and before "treatment"
    all_rules.sort(key=lambda x: len(x[0]), reverse=True)
    
    compiled = []
    for pattern_str, replacement in all_rules:
        try:
            compiled.append((re.compile(pattern_str, re.IGNORECASE), replacement))
        except re.error as e:
            print(f"[Compliance] Warning: Invalid regex '{pattern_str}': {e}")
    
    return compiled


# Module-level compiled patterns (built once at import time)
_MASTER_REPLACEMENTS = _build_master_list()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def sanitize_text(text: str) -> Tuple[str, List[str]]:
    """Apply all compliance replacements to a text string.
    
    Args:
        text: The input text to sanitize (product name, description, etc.)
    
    Returns:
        Tuple of (sanitized_text, list_of_changes_made)
        Each change is a string like "hair loss → hair care"
    """
    if not text:
        return text, []
    
    changes = []
    result = text
    
    for pattern, replacement in _MASTER_REPLACEMENTS:
        # Find all matches BEFORE replacing (to log changes)
        matches = pattern.findall(result)
        if matches:
            # Preserve case of first letter if replacement starts with lowercase
            def _case_preserving_replace(match):
                original = match.group(0)
                # If original starts uppercase and replacement starts lowercase, capitalize
                if original[0].isupper() and replacement[0].islower():
                    return replacement[0].upper() + replacement[1:]
                return replacement
            
            new_result = pattern.sub(_case_preserving_replace, result)
            if new_result != result:
                for m in matches:
                    original_text = m if isinstance(m, str) else m[0] if m else ''
                    if original_text:
                        changes.append(f"{original_text} → {replacement}")
                result = new_result
    
    # Clean up any double spaces or orphaned punctuation from replacements
    result = re.sub(r'  +', ' ', result)
    result = re.sub(r'\(\s*\)', '', result)
    result = re.sub(r'\[\s*\]', '', result)
    # Deduplicate adjacent identical words (e.g., "Cleansing Cleansing" → "Cleansing")
    result = re.sub(r'\b(\w+)\s+\1\b', r'\1', result, flags=re.IGNORECASE)
    result = result.strip()
    
    return result, changes


def sanitize_product(product: dict) -> Tuple[dict, Dict[str, List[str]]]:
    """Sanitize all text fields of a product dict for Coupang compliance.
    
    Applies keyword replacement to:
    - Product Name
    - Detailed Description
    - Search Keywords
    - Brand (light touch — only banned ingredient names)
    
    Args:
        product: Product dict with text fields
    
    Returns:
        Tuple of (sanitized_product, changes_by_field)
        changes_by_field maps field names to lists of change descriptions
    """
    p = product.copy()
    all_changes: Dict[str, List[str]] = {}
    
    # Fields to sanitize (in processing order)
    text_fields = ['Product Name', 'Detailed Description', 'Search Keywords']
    
    for field in text_fields:
        if p.get(field):
            sanitized, changes = sanitize_text(p[field])
            if changes:
                p[field] = sanitized
                all_changes[field] = changes
    
    # Light-touch Brand sanitization (only banned ingredients, not marketing terms)
    if p.get('Brand'):
        brand_text = p['Brand']
        for pattern, replacement in _build_ingredients_only():
            new_brand = pattern.sub(replacement, brand_text)
            if new_brand != brand_text:
                all_changes.setdefault('Brand', []).append(f"Brand ingredient replaced")
                brand_text = new_brand
        p['Brand'] = brand_text
    
    # Store compliance metadata
    if all_changes:
        change_summary = []
        for field, field_changes in all_changes.items():
            for c in field_changes:
                change_summary.append(f"[{field}] {c}")
        p['_compliance_changes'] = '; '.join(change_summary[:10])  # Cap at 10 to avoid huge strings
    
    return p, all_changes


def _build_ingredients_only() -> List[Tuple[re.Pattern, str]]:
    """Return compiled patterns for ONLY banned ingredients (for brand field)."""
    compiled = []
    for pattern_str, replacement in BANNED_INGREDIENTS:
        try:
            compiled.append((re.compile(pattern_str, re.IGNORECASE), replacement))
        except re.error:
            pass
    return compiled


def get_compliance_summary() -> str:
    """Return a human-readable summary of all compliance rules for logging."""
    total_rules = len(_MASTER_REPLACEMENTS)
    categories = {
        'Product Name Patterns': len(BANNED_PRODUCT_NAME_PATTERNS),
        'Banned Ingredients': len(BANNED_INGREDIENTS),
        'Pharma Claims': len(PHARMA_CLAIMS),
        'False Advertising': len(FALSE_ADVERTISING),
        'Dental Claims': len(DENTAL_CLAIMS),
        'Cosmetic Claims': len(COSMETIC_CLAIMS),
        'Hangover Claims': len(HANGOVER_CLAIMS),
    }
    lines = [f"Coupang Compliance Filter: {total_rules} total rules"]
    for cat, count in categories.items():
        lines.append(f"  • {cat}: {count} rules")
    return '\n'.join(lines)


def get_banned_keywords_for_prompt() -> str:
    """Return a formatted list of banned keywords for use in LLM prompts.
    
    This is injected into the Gemini sanitization prompt so the LLM
    also avoids generating descriptions with banned terms.
    """
    # Group the most important banned terms for the LLM prompt
    key_terms = [
        # From actual suspensions
        "repair & protect (dental context)", "hangover remedy", "hangover cure",
        "hair loss", "hair fall", "hair growth", "hair regrowth", "anti-hair fall",
        "baldness", "bald", "receding hairline",
        # Ingredients
        "andrographis", "chanca piedra", "phyllanthus amarus", "phyllanthus",
        "ephedra", "yohimbe", "kava",
        # Pharma claims
        "cure", "treatment", "remedy", "therapeutic", "medicinal", "medicated",
        "pharmaceutical", "prescription", "medication", "drug",
        "clinically proven", "FDA approved", "medical grade",
        # Dental quasi-drug
        "sensitivity relief", "enamel repair", "cavity protection",
        "anti-bacterial", "antiseptic", "disinfectant",
        "gum treatment", "gum disease", "gingivitis",
        # False advertising
        "guaranteed results", "miracle", "100% effective",
        "anti-cancer", "anti-tumor", "lowers blood pressure",
        # Cosmetics
        "whitening", "anti-aging treatment", "anti-wrinkle treatment",
        "wrinkle removal", "acne treatment", "acne cure",
        "botox", "scar removal",
        # Medical devices
        "medical device", "surgical grade", "diagnostic",
        # Hangover
        "hangover", "detox", "liver detox", "alcohol metabolism",
    ]
    
    safe_alternatives = [
        "care & fresh", "after-party wellness", "after-party wellness",
        "hair care", "hair thinning", "hair nourishment", "hair nourishment", "hair strengthening",
        "hair thinning", "thinning hair", "thinning hair",
        "herbal blend", "herbal extract", "herbal extract", "herbal extract",
        "herbal extract", "herbal extract", "herbal extract",
        "supports", "care routine", "support", "soothing", "herbal", "enhanced",
        "wellness", "professional", "supplement", "wellness",
        "quality tested", "quality certified", "premium grade",
        "gentle comfort", "enamel care", "cavity care",
        "cleansing", "cleansing", "cleansing",
        "gum care", "gum concern", "gum sensitivity",
        "expected results", "premium", "highly effective",
        "wellness support", "wellness support", "wellness support",
        "brightening", "age-defying care", "smoothing care",
        "skin smoothing", "blemish care", "blemish care",
        "firming", "scar care",
        "wellness tool", "premium grade", "monitoring",
        "after-party", "cleanse", "liver cleanse", "natural metabolism",
    ]
    
    lines = []
    for banned, safe in zip(key_terms, safe_alternatives):
        lines.append(f'  - "{banned}" → replace with "{safe}"')
    
    return '\n'.join(lines)
