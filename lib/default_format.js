function baseFormat (ts, type, key) {
	const listKeys = key.split('.');
	const act = listKeys.slice(3, listKeys.length).join('.');

	return {
		'@timestamp': ts,
		type,
		ns: listKeys[0] || '',
		grp: listKeys[1] || '',
		tgt: listKeys[2] || '',
		act: act || ''
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
	const o = Object.assign(
		{},
		value,
		value.histogram,
		baseFormat(ts, type, key)
	);
	delete o.histogram;
	bucket.push(o);
}

module.exports = {
	counters,
	timers,
	gauges: counters,
	timer_data
};
