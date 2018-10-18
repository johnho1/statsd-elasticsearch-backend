const http = require('http');
const format = require('./default_format');

/*
 * Flush stats to ElasticSearch (http://www.elasticsearch.org/)
 *
 * To enable this backend, include 'elastic' in the backends
 * configuration array:
 *
 *   backends: ['./backends/elastic'] 
 *  (if the config file is in the statsd folder)
 *
 * A sample configuration can be found in exampleElasticConfig.js
 *
 * This backend supports the following config options:
 */
const conf = {
	/**
	 * @cfg {String} [host='localhost']
	 * Hostname or IP of Elasticsearch server.
	 */
	host: 'localhost',
	/**
	 * @cfg {Number} [port=9200]
	 * Port of Elasticsearch server.
	 */
	port: 9200,
	/**
	 * @cfg {String} [path='/']
	 * HTTP path of Elasticsearch server.
	 */
	path: '/',
	/**
	 * @cfg {String} [indexPrefix='statsd']
	 * Prefix of the dynamic index to be created.
	 */
	indexPrefix: 'statsd',
	/**
	 * @cfg {'year'|'month'|'day'|'hour'} [indexTimestamp='day']
	 * Timestamping format of the index.
	 */
	indexTimestamp: 'day'
	/**
	 * @cfg {String} [username]
	 */
	/**
	 * @cfg {String} [password]
	 */
};

// this will be instantiated to the logger
let lg;
let debug;

function pad (num) {
	return num < 10 ? ('0' + num) : num;
}

function getIndex () {
	const indexDate = new Date();

	const statsdIndex = [
		conf.indexPrefix + '-' + indexDate.getUTCFullYear()
	];

	const idxTs = conf.indexTimestamp;

	if (idxTs == 'month' || idxTs == 'day' || idxTs == 'hour') {
		statsdIndex.push(pad(indexDate.getUTCMonth() + 1));
	}

	if (idxTs == 'day' || idxTs == 'hour') {
		statsdIndex.push(pad(indexDate.getUTCDate()));
	}

	if (idxTs == 'hour') {
		statsdIndex.push(pad(indexDate.getUTCHours()));
	}

	return statsdIndex.join('.');
}

function esBulkInsert (metrics) {
	const statsdIndex = getIndex();

	const index = JSON.stringify({
		index: {
			_index: statsdIndex,
			_type: '_doc'
		}
	});

	const payload = [];
	Object.keys(metrics)
	.forEach(k => {
		metrics[k].forEach(item => {
			payload.push(index, JSON.stringify(item))
		});
	});
	const payloadString = payload.join('\n') + '\n';

	var optionsPost = {
		host: conf.host,
		port: conf.port,
		path: `${conf.path}${statsdIndex}/_bulk`,
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'Content-Length': payloadString.length
		}
	};

	if(conf.username && conf.password) {
		optionsPost.auth = `${conf.username}:${conf.password}`;
	}

	var req = http.request(optionsPost, function (res) {
		res.on('data', function (d) {
			if (res.statusCode != 200) {
				lg.log(`HTTP ${res.statusCode}: ${d}`, 'ERROR');
			}
		});
	})
	.on('error', function (err) {
		lg.log('Error with HTTP request, no stats flushed.', 'ERROR');
		console.log(err);
	});

	if (debug) {
		lg.log('ES payload:');
		lg.log(payloadString);
	}
	req.write(payloadString);
	req.end();
}

function flushStats (ts, metrics) {
	const formattedMetrics = {
		counters: [],
		timers: [],
		gauges: [],
		timer_data: []
	};

	ts = ts * 1000;

	let type = 'counters';
	for (const key in metrics[type]) {
		format[type](ts, type, key, metrics[type][key], formattedMetrics[type]);
	}

	type = 'gauges';
	for (const key in metrics[type]) {
		format[type](ts, type, key, metrics[type][key], formattedMetrics[type]);
	}

	type = 'timers';
	for (const key in metrics[type]) {
		format[type](ts, type, key, metrics[type][key], formattedMetrics[type]);
	}

	if (formattedMetrics[type].length > 0) {
		type = 'timer_data';
		for (const key in metrics[type]) {
			format[type](
				ts,
				type,
				key,
				metrics[type][key],
				formattedMetrics[type]
			);
		}
	}

	if (debug) {
		lg.log('metrics:');
		lg.log( JSON.stringify(metrics) );
	}

	esBulkInsert(formattedMetrics);

	if (debug) {
		const numStats = Object.keys(formattedMetrics)
		.reduce((acc, k) => acc + formattedMetrics[k].length, 0);
		lg.log(`flushed ${numStats} stats to ES`, 'DEBUG');
	}
}

exports.init = function esInit (startup_time, config, events, logger) {
	debug = config.debug;
	lg = logger;

	Object.assign(conf, config.elasticsearch);

	events.on('flush', flushStats);

	return true;
};
