function baseFormat (ts, type, key) {
	const [ns, grp, tgt, ...act] = key.split('.');
	return {
		'@timestamp': ts,
		type,
		ns: ns || '',
		grp: grp || '',
		tgt: tgt || '',
		act: act.join('.') || ''
	};
}

function counters (ts, type, key, val, bucket) {
	bucket.push(Object.assign({ val }, baseFormat(ts, type, key)));
}

function timers (ts, type, key, series, bucket) {
	const base = baseFormat(ts, type, key);
	for (const keyTimer in series) {
		bucket.push(Object.assign({ val: series[keyTimer] }, base));
	}
}

function timer_data (ts, type, key, value, bucket) {
	const { histogram, ...rest } = value
	bucket.push(Object.assign(rest, histogram, baseFormat(ts, type, key)))
}

module.exports = {
	counters,
	timers,
	gauges: counters,
	timer_data
};
