"""Section-aware token windows retaining exact source text and line locations."""
import re


def sections(text):
    heading = 'Document'
    start = 0
    # Markdown headings and common article delimiters keep separate articles apart.
    pattern = r'(?m)^(?:#{1,6}\s+.+|Article\s+\d+[^\n]*|={3,}\s*)$'
    for match in re.finditer(pattern, text):
        if text[start:match.start()].strip():
            yield heading, start, text[start:match.start()]
        heading = match.group().strip('#= \r\n') or 'Article'
        start = match.start()
    if text[start:].strip():
        yield heading, start, text[start:]


def chunk_document(document, tokenizer, max_tokens=192, overlap=32):
    if not 0 <= overlap < max_tokens:
        raise ValueError('Overlap must be smaller than chunk size')
    text = document['text']
    result = []
    for heading, base, section in sections(text):
        offsets = tokenizer(section, add_special_tokens=False, return_offsets_mapping=True,
                            truncation=False)['offset_mapping']
        for start in range(0, len(offsets), max_tokens-overlap):
            window = offsets[start:start+max_tokens]
            if not window: continue
            left, right = window[0][0], window[-1][1]
            content = section[left:right]
            # Retokenizing text slices can differ at wordpiece boundaries.
            # Shorten until the stored excerpt itself fits without truncation.
            while len(tokenizer.encode(content, add_special_tokens=False)) > max_tokens and len(window)>1:
                window = window[:-1]; right = window[-1][1]; content=section[left:right]
            result.append({'position':len(result), 'section':heading[:200],
                'text':content, 'line_start':text.count('\n',0,base+left)+1,
                'line_end':text.count('\n',0,base+right)+1,
                'token_count':len(tokenizer.encode(content,add_special_tokens=False))})
            if start+max_tokens >= len(offsets): break
    return result
