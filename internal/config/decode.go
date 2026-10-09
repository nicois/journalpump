// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package config

import (
	"fmt"
)

// newConfig builds the typed view from the normalized raw document. Every
// rejected value carries its config-key path so the operator can find it;
// Python either crashed mid-run on these or (for the few truthiness-coerced
// keys) silently ignored them — both are covered by the typed-decoding
// deviation (spec §6.1a).
func newConfig(raw map[string]any, searchTagOrders map[string][][]string) (*Config, error) {
	cfg := &Config{
		raw:         raw,
		Tags:        map[string]string{},
		readersBlob: map[string]any{},
	}
	var err error
	if cfg.LogLevel, err = decodeLogLevel(raw); err != nil {
		return nil, err
	}
	if cfg.Statsd, err = decodeStatsd(raw); err != nil {
		return nil, err
	}
	if cfg.Tags, err = decodeTags(raw); err != nil {
		return nil, err
	}
	if cfg.GeoipDatabase, err = decodeString(raw, "geoip_database"); err != nil {
		return nil, err
	}
	if cfg.MsgBufferMaxLength, err = decodeIntWithDefault(raw, "msg_buffer_max_length", "msg_buffer_max_length", DefaultMsgBufferMaxLength); err != nil {
		return nil, err
	}
	if cfg.MsgBufferMaxBytes, err = decodeIntWithDefault(raw, "msg_buffer_max_bytes", "msg_buffer_max_bytes", DefaultMsgBufferBytes); err != nil {
		return nil, err
	}
	if cfg.JSONStateFilePath, err = decodeOptionalString(raw, "json_state_file_path", "json_state_file_path"); err != nil {
		return nil, err
	}
	if cfg.JSONRunningConfigPath, err = decodeOptionalString(raw, "json_running_config_path", "json_running_config_path"); err != nil {
		return nil, err
	}
	if cfg.WriteRunningConfig, err = decodeBool(raw, "write_running_config"); err != nil {
		return nil, err
	}
	if cfg.MatchKey, err = decodeOptionalString(raw, "match_key", "match_key"); err != nil {
		return nil, err
	}
	cfg.MatchValue = raw["match_value"]
	if cfg.FieldFilters, err = decodeFieldFilters(raw); err != nil {
		return nil, err
	}
	if cfg.UnitLogLevels, err = decodeUnitLogLevels(raw); err != nil {
		return nil, err
	}
	if cfg.Readers, cfg.readersBlob, err = decodeReaders(raw, searchTagOrders); err != nil {
		return nil, err
	}
	return cfg, nil
}

func decodeLogLevel(raw map[string]any) (int64, error) {
	value, ok := raw["log_level"]
	if !ok {
		return DefaultLogLevel, nil
	}
	if level, isInt := asInt64(value); isInt {
		return level, nil
	}
	if name, isStr := value.(string); isStr {
		level, known := pythonLogLevels[name]
		if !known {
			return 0, fmt.Errorf("log_level: unknown log level name %q", name)
		}
		return level, nil
	}
	return 0, fmt.Errorf("log_level: must be a log level name or an integer")
}

func decodeStatsd(raw map[string]any) (StatsdConfig, error) {
	statsdConfig := StatsdConfig{Tags: map[string]string{}}
	value, ok := raw["statsd"]
	if !ok || isFalsy(value) {
		// Python: `stats = self.config.get("statsd") or {}` — any falsy
		// value means "no statsd", and an unset host/port means disabled.
		return statsdConfig, nil
	}
	stats, ok := value.(map[string]any)
	if !ok {
		return statsdConfig, fmt.Errorf("statsd: must be an object")
	}
	if stats["host"] != nil {
		host, err := asString(stats["host"], "statsd.host")
		if err != nil {
			return statsdConfig, err
		}
		statsdConfig.Host = host
	}
	if stats["port"] != nil {
		port, isInt := asInt64(stats["port"])
		if !isInt {
			return statsdConfig, fmt.Errorf("statsd.port: must be an integer")
		}
		statsdConfig.Port = &port
	}
	if stats["prefix"] != nil {
		prefix, err := asString(stats["prefix"], "statsd.prefix")
		if err != nil {
			return statsdConfig, err
		}
		statsdConfig.Prefix = prefix
	}
	tags, err := decodeStringMap(stats, "tags", "statsd.tags")
	if err != nil {
		return statsdConfig, err
	}
	if tags != nil {
		statsdConfig.Tags = tags
	}
	return statsdConfig, nil
}

func decodeTags(raw map[string]any) (map[string]string, error) {
	value, ok := raw["tags"]
	if !ok {
		return map[string]string{}, nil
	}
	tags, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tags: must be an object")
	}
	result := map[string]string{}
	for key, val := range tags {
		// Python renders global tag values straight into the statsd
		// datagram (f-string); only string and integer values are
		// unambiguous there, so anything else is rejected.
		switch typed := val.(type) {
		case string:
			result[key] = typed
		case int64:
			result[key] = fmt.Sprintf("%d", typed)
		default:
			return nil, fmt.Errorf("tags.%s: must be a string", key)
		}
	}
	return result, nil
}

func decodeFieldFilters(raw map[string]any) (map[string]FieldFilterConfig, error) {
	filters := map[string]FieldFilterConfig{}
	value, ok := raw["field_filters"]
	if !ok {
		return filters, nil
	}
	rawFilters, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field_filters: must be an object")
	}
	for name, entryValue := range rawFilters {
		entry, ok := entryValue.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field_filters.%s: must be an object", name)
		}
		fieldsValue, ok := entry["fields"]
		if !ok {
			// Python: config["fields"] raises KeyError at load.
			return nil, fmt.Errorf("field_filters.%s: 'fields' is required", name)
		}
		rawFields, ok := fieldsValue.([]any)
		if !ok {
			return nil, fmt.Errorf("field_filters.%s.fields: must be a list", name)
		}
		fields := make([]string, 0, len(rawFields))
		for i, fieldValue := range rawFields {
			field, err := asString(fieldValue, fmt.Sprintf("field_filters.%s.fields[%d]", name, i))
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}
		whitelist := true
		if typeValue, present := entry["type"]; present {
			if typeValue == nil {
				// Python: config.get("type", "whitelist") == "whitelist"
				// evaluates None == "whitelist" -> False, i.e. blacklist
				// mode; a present-but-null type is NOT the default.
				whitelist = false
			} else {
				typeName, err := asString(typeValue, fmt.Sprintf("field_filters.%s.type", name))
				if err != nil {
					return nil, err
				}
				// Python: config.get("type", "whitelist") == "whitelist".
				whitelist = typeName == "whitelist"
			}
		}
		filters[name] = FieldFilterConfig{Name: name, Whitelist: whitelist, Fields: fields}
	}
	return filters, nil
}

func decodeUnitLogLevels(raw map[string]any) (map[string][]UnitLogLevelEntry, error) {
	levels := map[string][]UnitLogLevelEntry{}
	value, ok := raw["unit_log_levels"]
	if !ok {
		return levels, nil
	}
	rawLevels, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unit_log_levels: must be an object")
	}
	for name, entryValue := range rawLevels {
		entries, ok := entryValue.([]any)
		if !ok {
			return nil, fmt.Errorf("unit_log_levels.%s: must be a list", name)
		}
		parsed := make([]UnitLogLevelEntry, 0, len(entries))
		for i, item := range entries {
			entry, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("unit_log_levels.%s[%d]: must be an object", name, i)
			}
			glob, err := requiredString(entry, "service_glob", fmt.Sprintf("unit_log_levels.%s[%d]", name, i))
			if err != nil {
				return nil, err
			}
			logLevel, err := requiredString(entry, "log_level", fmt.Sprintf("unit_log_levels.%s[%d]", name, i))
			if err != nil {
				return nil, err
			}
			if _, known := SeverityLevels[logLevel]; !known {
				// Python raises KeyError from LOG_SEVERITY_MAPPING when the
				// level is first used (mid-run); reject at load instead.
				return nil, fmt.Errorf("unit_log_levels.%s[%d].log_level: unknown log level %q", name, i, logLevel)
			}
			parsed = append(parsed, UnitLogLevelEntry{ServiceGlob: glob, LogLevel: logLevel})
		}
		levels[name] = parsed
	}
	return levels, nil
}

func decodeReaders(raw map[string]any, searchTagOrders map[string][][]string) (map[string]ReaderConfig, map[string]any, error) {
	readers := map[string]ReaderConfig{}
	value, ok := raw["readers"]
	if !ok {
		// Python: config.get("readers", {}).
		return readers, map[string]any{}, nil
	}
	blob, ok := value.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("readers: must be an object")
	}
	for name, entryValue := range blob {
		entry, ok := entryValue.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("readers.%s: must be an object", name)
		}
		reader, err := decodeReader(name, entry, searchTagOrders[name])
		if err != nil {
			return nil, nil, err
		}
		readers[name] = reader
	}
	return readers, blob, nil
}

func decodeReader(name string, entry map[string]any, searchTagOrder [][]string) (ReaderConfig, error) {
	reader := ReaderConfig{Name: name}
	var err error
	if reader.JournalFlagNames, reader.JournalFlagsInt, err = decodeJournalFlags(entry, name); err != nil {
		return reader, err
	}
	if reader.JournalPath, err = decodeOptionalString(entry, "journal_path", readerPath(name)+".journal_path"); err != nil {
		return reader, err
	}
	if reader.JournalFiles, err = decodeOptionalStringList(entry, "journal_files", readerPath(name)); err != nil {
		return reader, err
	}
	if reader.JournalNamespace, err = decodeOptionalString(entry, "journal_namespace", readerPath(name)+".journal_namespace"); err != nil {
		return reader, err
	}
	if reader.UnitsToMatch, err = decodeStringList(entry, "units_to_match", readerPath(name)); err != nil {
		return reader, err
	}
	if reader.JournaldFilters, err = decodeJournaldFilters(entry, name); err != nil {
		return reader, err
	}
	if reader.InitialPosition, err = decodeInitialPosition(entry, name); err != nil {
		return reader, err
	}
	if reader.Searches, err = decodeSearches(entry, name); err != nil {
		return reader, err
	}
	reader.SearchTagOrder = searchTagOrder
	if reader.SecretFilters, err = decodeSecretFilters(entry, name); err != nil {
		return reader, err
	}
	// Python: config.get("secret_filter_metrics") is True — only JSON true
	// activates the metric; every other value is tolerated as false.
	reader.SecretFilterMetrics = entry["secret_filter_metrics"] == true
	if reader.ThresholdForMetricEmit, err = decodeIntWithDefault(entry, "threshold_for_metric_emit", readerPath(name)+".threshold_for_metric_emit", DefaultThresholdForMetricEmit); err != nil {
		return reader, err
	}
	if reader.MatchKey, err = decodeOptionalString(entry, "match_key", readerPath(name)+".match_key"); err != nil {
		return reader, err
	}
	reader.MatchValue = entry["match_value"]
	if reader.Senders, err = decodeSenders(entry, name); err != nil {
		return reader, err
	}
	return reader, nil
}

func readerPath(name string) string {
	return fmt.Sprintf("readers.%s", name)
}

func decodeJournalFlags(entry map[string]any, name string) ([]string, *int64, error) {
	value, ok := entry["journal_flags"]
	if !ok || value == nil {
		// Python passes flags=None to the journal reader (library default).
		return nil, nil, nil
	}
	switch typed := value.(type) {
	case []any:
		names := make([]string, 0, len(typed))
		for i, item := range typed {
			flag, err := asString(item, fmt.Sprintf("%s.journal_flags[%d]", readerPath(name), i))
			if err != nil {
				return nil, nil, err
			}
			names = append(names, flag)
		}
		return names, nil, nil
	case int64:
		flags := typed
		return nil, &flags, nil
	default:
		return nil, nil, fmt.Errorf("%s.journal_flags: must be a list of flag names or an integer", readerPath(name))
	}
}

func decodeJournaldFilters(entry map[string]any, name string) ([]map[string]any, error) {
	value, ok := entry["journald_filters"]
	if !ok {
		// Python: config.get("journald_filters", []).
		return nil, nil
	}
	filters, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s.journald_filters: must be a list", readerPath(name))
	}
	result := make([]map[string]any, 0, len(filters))
	for i, item := range filters {
		filter, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s.journald_filters[%d]: must be an object", readerPath(name), i)
		}
		result = append(result, filter)
	}
	return result, nil
}

func decodeInitialPosition(entry map[string]any, name string) (any, error) {
	value, ok := entry["initial_position"]
	if !ok || value == nil {
		return nil, nil
	}
	switch value.(type) {
	case string, int64:
		return value, nil
	default:
		return nil, fmt.Errorf("%s.initial_position: must be \"head\", \"tail\" or an integer", readerPath(name))
	}
}

func decodeSearches(entry map[string]any, name string) ([]any, error) {
	value, ok := entry["searches"]
	if !ok || isFalsy(value) {
		// Python: `searches or []` — any falsy value means no searches.
		return nil, nil
	}
	searches, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s.searches: must be a list", readerPath(name))
	}
	return searches, nil
}

func decodeSecretFilters(entry map[string]any, name string) ([]SecretFilterConfig, error) {
	value, ok := entry["secret_filters"]
	if !ok || value == nil {
		// Python: `if secret_filters is None: return []`.
		return nil, nil
	}
	rawFilters, ok := value.([]any)
	if !ok {
		// Python's exact ValueError message, raised at reader construction.
		return nil, fmt.Errorf("%s: invalid secret_filters configuration - must be a list", readerPath(name))
	}
	result := make([]SecretFilterConfig, 0, len(rawFilters))
	for i, item := range rawFilters {
		filter, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: invalid secret_filters configuration - must be a list", readerPath(name))
		}
		parsed := SecretFilterConfig{Raw: filter}
		// Python checks replacement first, then pattern, and raises
		// ValueError with these exact messages at reader construction; a
		// present-but-null field is missing (`.get(...) is None`).
		replacement, present := filter["replacement"]
		if !present || replacement == nil {
			return nil, fmt.Errorf("%s: invalid secret_filters configuration - missing field 'replacement'", readerPath(name))
		}
		pattern, present := filter["pattern"]
		if !present || pattern == nil {
			return nil, fmt.Errorf("%s: invalid secret_filters configuration - missing field 'pattern'", readerPath(name))
		}
		var err error
		if parsed.Replacement, err = asString(replacement, fmt.Sprintf("%s.secret_filters[%d].replacement", readerPath(name), i)); err != nil {
			return nil, err
		}
		if parsed.Pattern, err = asString(pattern, fmt.Sprintf("%s.secret_filters[%d].pattern", readerPath(name), i)); err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}

func decodeSenders(entry map[string]any, name string) (map[string]any, error) {
	value, ok := entry["senders"]
	if !ok {
		// Python: config.get("senders", {}).
		return map[string]any{}, nil
	}
	senders, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.senders: must be an object", readerPath(name))
	}
	for senderName, senderValue := range senders {
		if _, ok := senderValue.(map[string]any); !ok {
			return nil, fmt.Errorf("%s.senders.%s: must be an object", readerPath(name), senderName)
		}
	}
	return senders, nil
}

// decodeString decodes a required-to-be-string value (used for values with
// no key-path context in the error message).
func decodeString(raw map[string]any, key string) (string, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return "", nil
	}
	return asString(value, key)
}

// decodeOptionalString decodes a string-or-absent key; JSON null counts as
// absent, mirroring Python's `.get(key)` returning None.
func decodeOptionalString(raw map[string]any, key, path string) (string, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return "", nil
	}
	return asString(value, path)
}

func decodeBool(raw map[string]any, key string) (bool, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return false, nil
	}
	pythonBool, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s: must be a boolean", key)
	}
	return pythonBool, nil
}

func decodeIntWithDefault(raw map[string]any, key, path string, defaultValue int64) (int64, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return defaultValue, nil
	}
	number, isInt := asInt64(value)
	if !isInt {
		return 0, fmt.Errorf("%s: must be an integer", path)
	}
	return number, nil
}

func decodeStringList(raw map[string]any, key, path string) ([]string, error) {
	value, ok := raw[key]
	if !ok {
		// Python default: an empty list (config.get(key, [])).
		return []string{}, nil
	}
	return asStringList(value, fmt.Sprintf("%s.%s", path, key))
}

func decodeOptionalStringList(raw map[string]any, key, path string) ([]string, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		// Python passes None straight to the journal reader (library
		// default), so absent and null are equivalent.
		return nil, nil
	}
	return asStringList(value, fmt.Sprintf("%s.%s", path, key))
}

func asStringList(value any, path string) ([]string, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: must be a list of strings", path)
	}
	result := make([]string, 0, len(items))
	for i, item := range items {
		item, err := asString(item, fmt.Sprintf("%s[%d]", path, i))
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

// decodeStringMap decodes an optional object of string values. Values are
// coerced from integers only where Python tolerates it (via str()
// interpolation); nil result means "key absent".
func decodeStringMap(raw map[string]any, key, path string) (map[string]string, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return nil, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: must be an object", path)
	}
	result := map[string]string{}
	for name, item := range object {
		switch typed := item.(type) {
		case string:
			result[name] = typed
		case int64:
			result[name] = fmt.Sprintf("%d", typed)
		default:
			return nil, fmt.Errorf("%s.%s: must be a string", path, name)
		}
	}
	return result, nil
}

func requiredString(entry map[string]any, key, path string) (string, error) {
	value, ok := entry[key]
	if !ok {
		// Python: level["service_glob"] raises KeyError at load.
		return "", fmt.Errorf("%s: '%s' is required", path, key)
	}
	return asString(value, fmt.Sprintf("%s.%s", path, key))
}

func asString(value any, path string) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s: must be a string", path)
	}
	return text, nil
}
