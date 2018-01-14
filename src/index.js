/*
 * moleculer-db-adapter-couchbase
 * MIT Licensed
 */

"use strict";

const _ 		= require("lodash");
const Promise	= require("bluebird");
const Couchbase = require('couchbase');
const ottoman 	= require('ottoman');

class CouchbaseDbAdapter {
	/**
	 * Creates an instance of CouchbaseDbAdapter.
	 * @param {any} opts
	 *
	 * @memberof CouchbaseDbAdapter
	 */
	constructor(...opts) {
		this.opts = opts;
	}
	
	/**
	* Initialize adapter
	*
	* @param {ServiceBroker} broker
	* @param {Service} service
	*
	* @memberof CouchbaseDbAdapter
	*/
   init(broker, service) {
	   this.broker = broker;
	   this.service = service;

	   if (!this.service.schema.model) {
		   /* istanbul ignore next */
		   throw new Error("Missing `model` definition in schema of service!");
	   }
   }

   	/**
	 * Connect to database
	 *
	 * @returns {Promise}
	 *
	 * @memberof CouchbaseDbAdapter
	 */
	connect() {
		var cluster = new Couchbase.Cluster(this.opts.clutser || "couchbase://localhost/", this.opts.certPath || null);
		
		return new Promise((resolve, reject) => {
			this.bucket = cluster.openBucket(this.opts.bucketName || "default", this.opts.bucketPassword || null, (err) => {
				if (err) {
					return reject(err)
				}
				this.db = ottoman;
				this.db.bucket = this.bucket;
				let m = this.service.schema.model;
				this.model = ottoman.model(m.name, m.define, m.options);
				resolve()
			})
		});
		/*
		return this.db.authenticate().then(() => {

			let m = this.service.schema.model;
			this.model = this.db.define(m.name, m.define, m.options);
			this.service.model = this.model;

			return this.model.sync();
		});
		*/	
	}

	/**
	* Disconnect from database
	*
	* @returns {Promise}
	*
	* @memberof CouchbaseDbAdapter
	*/
	disconnect() {
		if (this.db) {
			this.db.disconnect();
		}
		return Promise.resolve();
	}

	/**
	 * Get count of filtered entites.
	 *
	 * Available query props:
	 *  - search
	 *  - searchFields
	 *  - query
	 *
	 * @param {Object} [filters={}]
	 * @returns {Promise<Number>} Return with the count of documents.
	 *
	 * @memberof MongoDbAdapter
	 */
	count(filters = {}) {
		return new Promise((resolve, reject) => {
			this.model.count(filters, (res, err) => {
				if (err) {
					return reject(err);
				}
				resolve(res);
			})
		})
	}

	/**
	 * Find an entities by ID.
	 *
	 * @param {String} _id
	 * @returns {Promise<Object>} Return with the found document.
	 *
	 * @memberof CouchbaseDbAdapter
	 */
	findById(_id) {
		return new Promise((resolve, reject) => {
			this.model.getById(_id, (res, err) => {
				if (err) {
					return reject(err);
				}
				resolve(res);
			});
		});
	}

	/**
	 * Find any entities by IDs.
	 *
	 * @param {Array} idList
	 * @returns {Promise<Array>} Return with the found documents in an Array.
	 *
	 * @memberof CouchbaseDbAdapter
	 */
	findByIds(idList) {
		return Promise.all(idList, this.findById);
	}

	/**
	 * Insert an entity.
	 *
	 * @param {Object} entity
	 * @returns {Promise<Object>} Return with the inserted document.
	 *
	 * @memberof CouchbaseDbAdapter
	 */
	insert(entity) {
		return new Promise((resolve, reject) => {
			new this.model(entity).save((err) => {
				if (err) {
					return reject(err);
				}
				resolve(this.model);
			});
		});
	}
}

module.exports = CouchbaseDbAdapter;
