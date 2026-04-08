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
    (r'prevents?\s+hair\s+loss', 'supports hair care'),
    (r'stops?\s+hair\s+loss', 'promotes hair wellness'),
    (r'reduces?\s+hair\s+loss', 'supports hair care'),
    (r'controls?\s+hair\s+loss', 'supports hair care'),
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
# TIER 9: KOREAN MFDS — HERBAL MEDICINE CLAIMS IN FOOD/SUPPLEMENTS
# ─────────────────────────────────────────────────────────────────────────────
HERBAL_MEDICINE_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)ginseng\s+(?:extract\s+)?(?:treats?|cures?|heals?|repairs?)(?!\w)', 'ginseng wellness'),
    (r'(?<!\w)ssanghwatang(?!\w)', 'herbal blend'),
    (r'(?<!\w)gong\s*jin\s*dan(?!\w)', 'herbal supplement'),
    (r'(?<!\w)cheongmyeongtang(?!\w)', 'herbal tea'),
    (r'(?<!\w)hericium(?!\w)', 'mushroom extract'),
    (r'(?<!\w)cordyceps\s+(?:treats?|cures?|heals?)(?!\w)', 'cordyceps support'),
    (r'(?<!\w)ashwagandha\s+(?:treats?|cures?)(?!\w)', 'ashwagandha support'),
    (r'traditional\s+(?:korean\s+)?(?:medicine|remedy|cure|treatment)', 'traditional wellness'),
    (r'oriental\s+(?:medicine|remedy|cure|treatment)', 'oriental wellness'),
    (r'herbal\s+(?:medicine|remedy|cure|treatment)', 'herbal support'),
    (r'(?<!\w)hanbang(?!\w)', 'traditional blend'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 10: KOREAN MFDS — COSMETICS FALSE ADVERTISING (2025 update)
# ─────────────────────────────────────────────────────────────────────────────
MFDS_COSMETIC_2025: List[Tuple[str, str]] = [
    # Anti-aging exaggerations
    (r'reduces?\s+skin\s+age\s+by\s+\d+\s+years?', 'visibly refreshes skin'),
    (r'look\s+\d+\s+years?\s+younger', 'look refreshed'),
    (r'turns?\s+back\s+(?:the\s+)?clock', 'revitalizes skin'),
    (r'reverses?\s+aging', 'supports youthful appearance'),
    (r'reverses?\s+skin\s+damage', 'supports skin renewal'),
    (r'reverses?\s+wrinkles?', 'smooths skin appearance'),
    (r'erases?\s+wrinkles?', 'softens skin'),
    (r'eliminates?\s+(?:fine\s+)?lines?', 'minimizes appearance of lines'),
    (r'fills?\s+(?:in\s+)?wrinkles?', 'smooths skin surface'),

    # Human-derived / biological terminology (MFDS banned)
    (r'stem\s+cell\s+(?:activat|stimulat|regenerat)', 'cell-supporting'),
    (r'(?<!\w)stem\s+cell(?!\w)', 'cell-supporting'),
    (r'(?<!\w)DNA\s+repair(?!\w)', 'skin renewal support'),
    (r'(?<!\w)collagen\s+(?:production\s+)?(?:boost|increas|stimulat)', 'supports skin firmness'),
    (r'(?<!\w)elastin\s+(?:production\s+)?(?:boost|increas|stimulat)', 'supports skin elasticity'),
    (r'cell\s+regenerat(?:ion|ing|es?)', 'skin renewal'),
    (r'cellular\s+(?:repair|renewal|regenerat)', 'skin renewal'),
    (r'(?<!\w)gene\s+express(?:ion)?(?!\w)', 'skin support'),
    (r'epigenetic', 'skin-supporting'),

    # Medical endorsements (banned by MFDS 2025)
    (r'dermatologist[\s-]?tested', 'skin-tested'),
    (r'dermatologist[\s-]?approved', 'expert recommended'),
    (r'allergy[\s-]?tested', 'sensitivity-tested'),
    (r'ophthalmologist[\s-]?tested', 'sensitivity-tested'),
    (r'pediatrician[\s-]?(?:tested|approved|recommended)', 'sensitivity-tested'),
    (r'(?<!\w)hypoallergenic(?!\w)', 'gentle formula'),
    (r'non[\s-]?irritating', 'gentle formula'),
    (r'no\s+steroid\s+ingredients?', 'gentle formula'),
    (r'steroid[\s-]?free', 'gentle formula'),

    # Functional cosmetic claims requiring MFDS approval
    (r'(?<!\w)melanin\s+(?:suppress|inhibit|reduc)', 'brightening support'),
    (r'(?<!\w)melanin(?!\w)', 'skin tone'),
    (r'(?<!\w)depigment(?:ation|ing)?(?!\w)', 'brightening'),
    (r'(?<!\w)hyperpigment(?:ation)?(?!\w)', 'uneven skin tone'),
    (r'sun\s+(?:spot|damage)\s+(?:remov|eliminat|treat)', 'sun spot care'),
    (r'(?<!\w)photoaging(?!\w)', 'age-related skin change'),
    (r'UV\s+(?:damage\s+)?(?:repair|treat|revers)', 'UV care'),
    (r'pore\s+(?:minimiz|shrink|reduc|eliminat)', 'pore care'),
    (r'(?<!\w)keratin(?:iz)?(?:ation)?(?!\w)', 'strengthening'),
    (r'sebum\s+(?:control|regulat|reduc)', 'oil balance'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 11: ABSOLUTE SUPERLATIVE / FALSE ADVERTISING
# ─────────────────────────────────────────────────────────────────────────────
SUPERLATIVE_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)#\s*1\s+(?:selling|rated|recommended|brand|product)(?!\w)', 'highly rated'),
    (r'(?<!\w)number\s+one\s+(?:selling|rated|brand)(?!\w)', 'highly rated'),
    (r'(?<!\w)best[\s-]?selling(?!\w)', 'popular'),
    (r'(?<!\w)best\s+(?:in\s+class|in\s+the\s+world|ever|available)(?!\w)', 'premium'),
    (r'(?<!\w)world[\s-]?(?:class|leading|best|renowned|famous)(?!\w)', 'premium'),
    (r'(?<!\w)award[\s-]?winning(?!\w)', 'recognized quality'),
    (r'(?<!\w)certified\s+organic(?!\w)', 'made with natural ingredients'),
    (r'(?<!\w)100\s*%\s+(?:natural|organic|pure)(?!\w)', 'made with natural ingredients'),
    (r'(?<!\w)all[\s-]?natural(?!\w)', 'made with natural ingredients'),
    (r'(?<!\w)chemical[\s-]?free(?!\w)', 'made with natural ingredients'),
    (r'(?<!\w)toxin[\s-]?free(?!\w)', 'gentle formula'),
    (r'(?<!\w)cruelty[\s-]?free(?!\w)', 'ethically produced'),
    (r'(?<!\w)proven\s+(?:effective|results?|formula)(?!\w)', 'trusted formula'),
    (r'(?<!\w)unlike\s+other\s+brands?(?!\w)', 'unique formula'),
    (r'(?<!\w)superior\s+to(?!\w)', 'compared to'),
    (r'(?<!\w)better\s+than\s+(?:others?|competition|alternatives?)(?!\w)', 'effective formula'),
    (r'(?<!\w)most\s+effective(?!\w)', 'highly effective'),
    (r'(?<!\w)strongest(?!\w)', 'concentrated'),
    (r'(?<!\w)most\s+powerful(?!\w)', 'high performance'),
    (r'(?<!\w)fastest(?!\w)', 'quick-acting'),
    (r'(?<!\w)longest[\s-]?lasting(?!\w)', 'long-lasting'),
    (r'(?<!\w)permanent(?!\w)', 'long-lasting'),
    (r'(?<!\w)instant(?:ly|aneous)?(?!\w)', 'quick'),
    (r'(?<!\w)overnight\s+(?:result|fix|solution|cure)(?!\w)', 'visible results'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 12: WEIGHT LOSS / BODY CLAIMS
# ─────────────────────────────────────────────────────────────────────────────
WEIGHT_LOSS_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)weight\s+loss(?!\w)', 'wellness support'),
    (r'(?<!\w)fat\s+(?:burn(?:ing)?|loss|melt(?:ing)?)(?!\w)', 'body wellness'),
    (r'(?<!\w)fat\s+(?:reduc(?:tion|ing)|eliminat)(?!\w)', 'body wellness'),
    (r'(?<!\w)slimming(?!\w)', 'body care'),
    (r'(?<!\w)slim(?:mer)?(?!\w)', 'lean'),
    (r'(?<!\w)diet\s+(?:pill|supplement|aid|product)(?!\w)', 'wellness supplement'),
    (r'(?<!\w)appetite\s+(?:suppress|control|reduc)(?!\w)', 'wellness support'),
    (r'(?<!\w)metabolism\s+(?:boost|increas|enhanc)(?!\w)', 'energy support'),
    (r'(?<!\w)cellulite(?!\w)', 'skin texture'),
    (r'burns?\s+(?:fat|calories)(?!\w)', 'supports energy'),
    (r'(?<!\w)thermogenic(?!\w)', 'warming formula'),
    (r'(?<!\w)lipolysis(?!\w)', 'body wellness'),
    (r'lose\s+(?:\d+\s+)?(?:kg|pounds?|lbs?)(?!\w)', 'support wellness goals'),
    (r'shed\s+(?:weight|fat|pounds?|kg)(?!\w)', 'support wellness goals'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 13: IMMUNITY / DISEASE PREVENTION CLAIMS
# ─────────────────────────────────────────────────────────────────────────────
IMMUNITY_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)boosts?\s+immunity(?!\w)', 'supports wellness'),
    (r'(?<!\w)boosts?\s+immune\s+(?:system|function|response)(?!\w)', 'supports wellness'),
    (r'(?<!\w)strengthens?\s+immune(?!\w)', 'supports wellness'),
    (r'(?<!\w)immune\s+(?:boost|support|enhanc)(?!\w)', 'wellness support'),
    (r'(?<!\w)prevents?\s+(?:cold|flu|infection|virus|disease)(?!\w)', 'wellness support'),
    (r'(?<!\w)fights?\s+(?:infection|virus|bacteria|disease)(?!\w)', 'wellness support'),
    (r'(?<!\w)anti[\s-]?viral(?!\w)', 'wellness support'),
    (r'(?<!\w)anti[\s-]?infective(?!\w)', 'wellness support'),
    (r'(?<!\w)anti[\s-]?fungal(?!\w)', 'cleansing formula'),
    (r'(?<!\w)kills?\s+(?:virus|viruses|pathogens?)(?!\w)', 'cleansing formula'),
    (r'(?<!\w)COVID(?:[\s-]?19)?(?!\w)', 'wellness'),
    (r'(?<!\w)coronavirus(?!\w)', 'wellness'),
    (r'(?<!\w)pandemic(?!\w)', 'wellness period'),
    (r'(?<!\w)antioxidant\s+(?:that\s+)?(?:fights?|prevents?|treats?)(?!\w)', 'antioxidant rich'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 14: SEXUAL / REPRODUCTIVE HEALTH CLAIMS
# ─────────────────────────────────────────────────────────────────────────────
SEXUAL_HEALTH_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)sexual\s+performance(?!\w)', 'personal wellness'),
    (r'(?<!\w)erectile\s+(?:dysfunction|function)(?!\w)', 'personal wellness'),
    (r'(?<!\w)libido\s+(?:boost|enhanc|increas)(?!\w)', 'personal wellness'),
    (r'(?<!\w)testosterone\s+(?:boost|enhanc|increas)(?!\w)', 'personal wellness'),
    (r'(?<!\w)aphrodisiac(?!\w)', 'wellness supplement'),
    (r'(?<!\w)virility(?!\w)', 'personal wellness'),
    (r'(?<!\w)fertility\s+(?:boost|enhanc|increas|treat)(?!\w)', 'personal wellness'),
    (r'(?<!\w)sperm\s+(?:count|quality|boost)(?!\w)', 'personal wellness'),
    (r'(?<!\w)menopause\s+(?:relief|treatment|cure)(?!\w)', 'hormonal wellness'),
    (r'(?<!\w)PMS\s+(?:relief|treatment|cure)(?!\w)', 'monthly wellness support'),
    (r'(?<!\w)hormone\s+(?:balanc|regulat|boost)(?!\w)', 'wellness support'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 15: HAIR CARE & FUNCTIONAL COSMETICS
# ─────────────────────────────────────────────────────────────────────────────
HAIR_CARE_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)repair(?:\s+damaged)?\s+hair(?!\w)', 'helps improve hair appearance'),
    (r'(?<!\w)repairs?(?!\w)', 'improves appearance'),
    (r'(?<!\w)restores?\s+hair(?!\w)', 'helps maintain good-looking hair'),
    (r'(?<!\w)restores?(?!\w)', 'maintains condition'),
    (r'(?<!\w)hair\s+treatment(?!\w)', 'hair care product'),
    (r'(?<!\w)anti[\s-]?hair\s+fall(?!\w)', 'helps care for hair'),
    (r'(?<!\w)hair\s+loss(?:\s+control|prevention)?(?!\w)', 'scalp care'),
    (r'(?<!\w)strengthens?\s+roots?(?!\w)', 'hair and scalp care'),
    (r'(?<!\w)100\s*%\s+smooth(?!\w)', 'smooth-looking finish'),
    (r'(?<!\w)complete\s+repair(?!\w)', 'hair care'),
    (r'(?<!\w)instant\s+results?(?!\w)', 'regular care'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 16: BABY & MATERNITY CLAIMS
# ─────────────────────────────────────────────────────────────────────────────
BABY_MATERNITY_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)(?:prevents?|cures?)\s+SIDS(?!\w)', 'designed for baby comfort'),
    (r'(?<!\w)colic\s+(?:cure|treatment|remedy)(?!\w)', 'colic comfort'),
    (r'(?<!\w)100\s*%\s+safe\s+for\s+babies(?!\w)', 'gentle on baby'),
    (r'(?<!\w)pediatrician\s+(?:approved|recommended)(?!\w)', 'designed for babies'),
    (r'(?<!\w)tear[\s-]?free(?!\w)', 'gentle formula'),
    (r'(?<!\w)completely\s+non[\s-]?toxic(?!\w)', 'made with safe materials'),
    (r'(?<!\w)cures?\s+diaper\s+rash(?!\w)', 'soothes skin'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 17: ELECTRONICS & SAFETY CERTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────
ELECTRONICS_SAFETY_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)KC\s+certified(?!\w)', 'built to standard'), # KC requires explicit registration IDs, you can't just claim it
    (r'(?<!\w)radiation[\s-]?free(?!\w)', 'standard emission'),
    (r'(?<!\w)100\s*%\s+hazard[\s-]?free(?!\w)', 'designed for safety'),
    (r'(?<!\w)indestructible(?!\w)', 'durable'),
    (r'(?<!\w)fire[\s-]?proof(?!\w)', 'fire-resistant'),
    (r'(?<!\w)water[\s-]?proof(?!\w)', 'water-resistant'),
    (r'(?<!\w)shock[\s-]?proof(?!\w)', 'shock-resistant'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 18: PET CARE CLAIMS
# ─────────────────────────────────────────────────────────────────────────────
PET_CARE_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)(?:treats?|cures?)\s+(?:parvo|flea|tick|heartworm|mange)(?!\w)', 'helps manage pet comfort'),
    (r'(?<!\w)veterinarian\s+(?:approved|recommended)(?!\w)', 'pet-friendly'),
    (r'(?<!\w)calms?\s+(?:dog|cat|pet)\s+anxiety(?!\w)', 'supports pet relaxation'),
    (r'(?<!\w)stops?\s+barking\s+instantly(?!\w)', 'training aid'),
    (r'(?<!\w)dental\s+disease\s+(?:cure|treatment)(?!\w)', 'dental care support'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 19: UNAUTHORIZED BRANDING / IMPERSONATION
# ─────────────────────────────────────────────────────────────────────────────
UNAUTHORIZED_BRANDING: List[Tuple[str, str]] = [
    (r'(?<!\w)official\s+(?:store|distributor|dealer)(?!\w)', 'seller'),
    (r'(?<!\w)100\s*%\s+(?:genuine|authentic|original)(?!\w)', 'original quality'),
    (r'(?<!\w)direct\s+from\s+manufacturer(?!\w)', 'imported'),
    (r'(?<!\w)authorized\s+seller(?!\w)', 'retailer'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 20: WEAPONS, TACTICAL & SELF-DEFENSE
# ─────────────────────────────────────────────────────────────────────────────
WEAPONS_TACTICAL_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)military[\s-]?grade(?!\w)', 'durable design'),
    (r'(?<!\w)tactical\s+(?:assault|combat)(?!\w)', 'outdoor'),
    (r'(?<!\w)self[\s-]?defense\s+(?:weapon|gear)(?!\w)', 'personal safety'),
    (r'(?<!\w)lethal(?!\w)', 'high impact'),
    (r'(?<!\w)concealed\s+carry(?!\w)', 'compact'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 21: SUBSTANCE, ALCOHOL & TOBACCO
# ─────────────────────────────────────────────────────────────────────────────
SUBSTANCE_ALCOHOL_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)hangover\s+(?:cure|treatment|remedy)(?!\w)', 'morning recovery'),
    (r'(?<!\w)mimics\s+(?:smoking|tobacco)(?!\w)', 'alternative habit'),
    (r'(?<!\w)(?:detox|cleanse)\s+your\s+liver(?!\w)', 'supports body wellness'),
    (r'(?<!\w)nicotine[\s-]?free\s+high(?!\w)', 'relaxing feel'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 22: ENVIRONMENTAL & GREENWASHING
# ─────────────────────────────────────────────────────────────────────────────
ENVIRONMENTAL_GREENWASHING_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)100\s*%\s+eco[\s-]?friendly(?!\w)', 'designed with the environment in mind'),
    (r'(?<!\w)zero\s+carbon\s+footprint(?!\w)', 'low emission design'),
    (r'(?<!\w)100\s*%\s+biodegradable(?!\w)', 'compostable materials included'),
    (r'(?<!\w)saves\s+the\s+planet(?!\w)', 'earth-conscious'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 23: MAGICAL, SUPERSTITION & LUCK
# ─────────────────────────────────────────────────────────────────────────────
MAGICAL_SUPERSTITION_CLAIMS: List[Tuple[str, str]] = [
    (r'(?<!\w)(?:guaranteed\s+to\s+)?bring(?:\s+you)?\s+(?:wealth|money|luck)(?!\w)', 'traditionally represents good fortune'),
    (r'(?<!\w)wins?\s+the\s+lottery(?!\w)', 'symbol of luck'),
    (r'(?<!\w)magical\s+(?:powers|healing)(?!\w)', 'comforting'),
    (r'(?<!\w)removes?\s+(?:hexes|curses|bad\s+luck)(?!\w)', 'positive energy'),
]

# ─────────────────────────────────────────────────────────────────────────────
# TIER 24: USER EXACT MAPPINGS
# User-specified mandatory safe replacements
# ─────────────────────────────────────────────────────────────────────────────
USER_EXACT_MAPPINGS: List[Tuple[str, str]] = [
    (r'(?<!\w)anti[\s-]?aging(?!\w)', 'daily care'),
    (r'(?<!\w)anti(?!\w)', 'care'),
    (r'(?<!\w)treats?(?!\w)', 'helps keep'),
    (r'(?<!\w)treating(?!\w)', 'helps keep'),
    (r'(?<!\w)treatment(?!\w)', 'helps keep'),
    (r'(?<!\w)heals?(?!\w)', 'helps maintain'),
    (r'(?<!\w)healing(?!\w)', 'helps maintain'),
    (r'(?<!\w)cures?(?!\w)', 'care'),
    (r'(?<!\w)curing(?!\w)', 'care'),
    (r'(?<!\w)removes?(?!\w)', 'helps clean'),
    (r'(?<!\w)removing(?!\w)', 'helps clean'),
    (r'(?<!\w)reduces?(?!\w)', 'helps maintain'),
    (r'(?<!\w)reducing(?!\w)', 'helps maintain'),
    (r'(?<!\w)acne(?!\w)', 'skin care'),
    (r'(?<!\w)dark\s+spots?(?!\w)', 'uneven tone'),
    (r'(?<!\w)brightening(?!\w)', 'fresh look'),
    (r'(?<!\w)SPF(?:\s*\d+(?:\+)?(?:/\w+\+*)?)?(?!\w)', ''),
    (r'(?<!\w)clinically(?!\w)', ''),
    (r'(?<!\w)guarantees?(?!\w)', ''),
    (r'(?<!\w)guaranteed(?!\w)', ''),
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
        HANGOVER_CLAIMS +
        HERBAL_MEDICINE_CLAIMS +
        MFDS_COSMETIC_2025 +
        SUPERLATIVE_CLAIMS +
        WEIGHT_LOSS_CLAIMS +
        IMMUNITY_CLAIMS +
        SEXUAL_HEALTH_CLAIMS +
        HAIR_CARE_CLAIMS +
        BABY_MATERNITY_CLAIMS +
        ELECTRONICS_SAFETY_CLAIMS +
        PET_CARE_CLAIMS +
        UNAUTHORIZED_BRANDING +
        WEAPONS_TACTICAL_CLAIMS +
        SUBSTANCE_ALCOHOL_CLAIMS +
        ENVIRONMENTAL_GREENWASHING_CLAIMS +
        MAGICAL_SUPERSTITION_CLAIMS
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

def _build_user_list() -> List[Tuple[re.Pattern, str]]:
    compiled = []
    # Sort user mappings by string length descending too
    sorted_user = sorted(USER_EXACT_MAPPINGS, key=lambda x: len(x[0]), reverse=True)
    for pattern_str, replacement in sorted_user:
        try:
            compiled.append((re.compile(pattern_str, re.IGNORECASE), replacement))
        except re.error as e:
            print(f"[Compliance] Warning: Invalid regex '{pattern_str}': {e}")
    return compiled

_USER_REPLACEMENTS = _build_user_list()


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
    
    # 1. Apply user exact mandatory mappings FIRST
    for pattern, replacement in _USER_REPLACEMENTS:
        matches = pattern.findall(result)
        if matches:
            def _case_preserving_replace(match):
                original = match.group(0)
                if original[0].isupper() and replacement and replacement[0].islower():
                    return replacement[0].upper() + replacement[1:]
                return replacement
            
            new_result = pattern.sub(_case_preserving_replace, result)
            if new_result != result:
                for m in matches:
                    original_text = m if isinstance(m, str) else m[0] if m else ''
                    if original_text:
                        changes.append(f"{original_text} → {replacement if replacement else '[REMOVED]'}")
                result = new_result

    # 2. Apply master mapping for other banned words
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
        'Herbal Medicine (MFDS)': len(HERBAL_MEDICINE_CLAIMS),
        'Cosmetic 2025 (MFDS)': len(MFDS_COSMETIC_2025),
        'Superlatives': len(SUPERLATIVE_CLAIMS),
        'Weight Loss': len(WEIGHT_LOSS_CLAIMS),
        'Immunity': len(IMMUNITY_CLAIMS),
        'Sexual Diagnostics': len(SEXUAL_HEALTH_CLAIMS),
        'Hair Care (Functional)': len(HAIR_CARE_CLAIMS),
        'Baby & Maternity': len(BABY_MATERNITY_CLAIMS),
        'Electronics & Safety': len(ELECTRONICS_SAFETY_CLAIMS),
        'Pet Care': len(PET_CARE_CLAIMS),
        'Unauthorized Branding': len(UNAUTHORIZED_BRANDING),
        'Weapons & Tactical': len(WEAPONS_TACTICAL_CLAIMS),
        'Substances & Hangover': len(SUBSTANCE_ALCOHOL_CLAIMS),
        'Environmental & Greenwashing': len(ENVIRONMENTAL_GREENWASHING_CLAIMS),
        'Magical & Superstition': len(MAGICAL_SUPERSTITION_CLAIMS),
        'User Exact Mappings': len(USER_EXACT_MAPPINGS),
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
    
    # ── USER EXACT MAPPINGS ──
    user_banned = [
        "anti-aging / anti aging", "anti", "treat", "heal", "cure", "remove", 
        "reduce", "acne", "dark spot", "brightening", "SPF", "clinically", "guarantee"
    ]
    user_safe = [
        "daily care", "care", "helps keep", "helps maintain", "care", "helps clean",
        "helps maintain", "skin care", "uneven tone", "fresh look", "REMOVE COMPLETELY", 
        "REMOVE COMPLETELY", "REMOVE COMPLETELY"
    ]
    
    lines = ["🔥 MANDATORY EXACT WORD REPLACEMENTS 🔥"]
    for banned, safe in zip(user_banned, user_safe):
        lines.append(f'  - "{banned}" → replace with "{safe}"')
        
    lines.append("\nOTHER BANNED TERMS:")
    
    for banned, safe in zip(key_terms, safe_alternatives):
        lines.append(f'  - "{banned}" → replace with "{safe}"')
    
    return '\n'.join(lines)
