"""Pure, lossless codec for canonical legacy lookup group JSON; no I/O."""
from functools import lru_cache
import hashlib
import json
import re

SETTINGS_KEY = 'market_lookup_group_dictionary:v1'
VERSION = 1
MAX_DICTIONARY_BYTES = 1024 * 1024
MAX_ENTRIES = 16384
MAX_ENCODED_ROW_BYTES = 1024 * 1024
_PREFIX = re.compile(r'^\s*\{\s*"\$g"\s*:\s*\[')


class CodecError(ValueError):
    pass


class DictionaryError(CodecError):
    pass


class ReferenceError(CodecError):
    pass


def _constant(value):
    raise ValueError('Non-finite JSON constant')


def _load(raw):
    return json.loads(raw, parse_constant=_constant)


def _compact(value):
    # Match legacy json.dumps(groups, separators=(',', ':')): ASCII escaping,
    # original object insertion order, ordered lists, duplicates and nulls.
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False)


def _serialized(entries):
    payload = {'version': VERSION, 'entries': list(entries)}
    checksum = hashlib.sha256(_compact(payload).encode('ascii')).hexdigest()
    return _compact({**payload, 'sha256': checksum})


_EMPTY_BYTES = len(_serialized(()))


def _mapping(value):
    if (type(value) is not dict or set(value) != {'version', 'entries', 'sha256'}
            or type(value['version']) is not int or value['version'] != VERSION
            or type(value['entries']) is not list or len(value['entries']) > MAX_ENTRIES
            or type(value['sha256']) is not str
            or re.fullmatch('[0-9a-f]{64}', value['sha256']) is None):
        raise DictionaryError('Unrecognized dictionary version, fields or types')
    entries = value['entries']
    seen = set()
    encoded_bytes = _EMPTY_BYTES + max(0, len(entries) - 1)
    try:
        for fragment in entries:
            if type(fragment) is not str or len(fragment) > MAX_DICTIONARY_BYTES:
                raise DictionaryError('Invalid dictionary fragment type or size')
            encoded_bytes += len(_compact(fragment))
            if encoded_bytes > MAX_DICTIONARY_BYTES:
                raise DictionaryError('Dictionary exceeds 1 MiB')
            if fragment in seen or _compact(_load(fragment)) != fragment:
                raise DictionaryError('Duplicate or noncanonical dictionary fragment')
            seen.add(fragment)
        serialized = _serialized(entries)
    except (ValueError, TypeError, RecursionError, OverflowError) as error:
        if isinstance(error, DictionaryError):
            raise
        raise DictionaryError('Invalid dictionary JSON fragment') from error
    if len(serialized) > MAX_DICTIONARY_BYTES:
        raise DictionaryError('Dictionary exceeds 1 MiB')
    if json.loads(serialized)['sha256'] != value['sha256']:
        raise DictionaryError('Dictionary checksum mismatch')
    return tuple(entries)


@lru_cache(maxsize=2)
def _text_dictionary(value):
    if len(value) > MAX_DICTIONARY_BYTES:
        raise DictionaryError('Dictionary exceeds 1 MiB')
    try:
        if len(value.encode('utf-8')) > MAX_DICTIONARY_BYTES:
            raise DictionaryError('Dictionary exceeds 1 MiB')
        parsed = _load(value)
    except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError) as error:
        if isinstance(error, DictionaryError):
            raise
        raise DictionaryError('Malformed dictionary JSON') from error
    return _mapping(parsed)


def _dictionary(value):
    if type(value) is str:
        return _text_dictionary(value)
    if type(value) is dict:
        return _mapping(value)
    raise DictionaryError('A validated dictionary value is required for compact references')


def _reference(raw):
    try:
        parsed = _load(raw)
    except (ValueError, TypeError, RecursionError, OverflowError) as error:
        if _PREFIX.match(raw):
            raise ReferenceError('Malformed compact group reference') from error
        return None
    if type(parsed) is dict and set(parsed) == {'$g'} and type(parsed['$g']) is list:
        return parsed['$g']
    return None


def is_encoded(raw):
    """Recognize the reserved envelope, including corrupt/truncated candidates."""
    if type(raw) is not str:
        return False
    try:
        return _reference(raw) is not None
    except ReferenceError:
        return True


class DictionaryBuilder:
    def __init__(self, value=None):
        self._entries = list(_dictionary(value)) if value is not None else []
        self._ids = {fragment: index + 1 for index, fragment in enumerate(self._entries)}
        self._bytes = _EMPTY_BYTES + sum(len(_compact(fragment)) for fragment in self._entries) + max(0, len(self._entries) - 1)

    def encode(self, raw):
        """Return compact references only after exact canonical roundtrip; else raw."""
        if type(raw) is not str:
            raise TypeError('groups_json must be a string')
        if len(raw) > MAX_ENCODED_ROW_BYTES:
            return raw
        try:
            tags = _load(raw)
            if type(tags) is not list or _compact(tags) != raw:
                return raw
            pending, additions, ids = {}, [], []
            size = self._bytes
            for tag in tags:
                fragment = _compact(tag)
                identifier = self._ids.get(fragment) or pending.get(fragment)
                if identifier is None:
                    count = len(self._entries) + len(additions)
                    if count >= MAX_ENTRIES:
                        return raw
                    size += len(_compact(fragment)) + int(count > 0)
                    if size > MAX_DICTIONARY_BYTES:
                        return raw
                    identifier = count + 1
                    pending[fragment] = identifier
                    additions.append(fragment)
                ids.append(identifier)
            encoded = _compact({'$g': ids})
            if len(encoded) > MAX_ENCODED_ROW_BYTES:
                return raw
        except (ValueError, TypeError, RecursionError, OverflowError):
            return raw
        # Capacity failures above never partially extend the dictionary.
        self._entries.extend(additions)
        self._ids.update(pending)
        self._bytes = size
        return encoded

    def serialized(self):
        value = _serialized(self._entries)
        if len(value) != self._bytes or len(value) > MAX_DICTIONARY_BYTES:
            raise DictionaryError('Dictionary size invariant failed')
        return value


def decode(raw, value):
    """Restore exact original groups_json; plain legacy/unknown objects pass through."""
    if type(raw) is not str:
        raise TypeError('groups_json must be a string')
    ids = _reference(raw)
    if ids is None:
        return raw
    entries = _dictionary(value)
    if len(raw) > MAX_ENCODED_ROW_BYTES or _compact({'$g': ids}) != raw:
        raise ReferenceError('Noncanonical or oversized compact reference')
    size = 2 + max(0, len(ids) - 1)
    fragments = []
    for identifier in ids:
        if type(identifier) is not int or not 1 <= identifier <= len(entries):
            raise ReferenceError('Unknown, malformed or out-of-range group tag ID')
        fragment = entries[identifier - 1]
        size += len(fragment)
        if size > MAX_ENCODED_ROW_BYTES:
            raise ReferenceError('Decoded compact row exceeds its encoding bound')
        fragments.append(fragment)
    return '[' + ','.join(fragments) + ']'
