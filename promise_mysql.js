const mysql = require('mysql');

class PromiseMysql {
	constructor(config) {
    this.connection = mysql.createConnection(config);
    this.connection.connect();
  }

  isConnected() {
    return this.connection && this.connection.state !== 'disconnected';
  }

  beiginTransaction() {
    return new Promise((resolve, reject) => {
      this.connection.beiginTransaction(error => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  query(sql, params) {
    return new Promise((resolve, reject) => {
      this.connection.query(sql, params, (error, results, fields) => {
        if (error) {
          reject(error);
        } else {
          resolve(results);
        }
      });
    });
  }

  commit() {
    return new Promise((resolve, reject) => {
      this.connection.commit(error => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }
}