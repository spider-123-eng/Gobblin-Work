package com.gobblin.sequence.hdfs;

import java.io.Serializable;

class Tuple<K, V> implements Serializable {
	private static final long serialVersionUID = 1L;
	private final K key;
	private final V value;

	public Tuple(K ke, V val) {
		key = ke;
		value = val;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public String toString() {
		return String.format("KEY: '%s', VALUE: '%s'", key, value);
	}

}
