const config = require('./config.json');
const fs = require('fs');
const mysql = require('mysql');

const insertBroth = 'INSERT INTO broth (description,rating) VALUES(?,?)';
const insertNoodles = 'INSERT INTO noodles (description,rating) VALUES(?,?)';
const insertToppings = 'INSERT INTO toppings (description,rating) VALUES(?,?)';
const insertRestaurant = 'INSERT INTO restaurant ' + 
  '(restaurant_name,website,locality,administrative_area,country,lat_lng) ' + 
  'VALUES(?,?,?,?,?,GeomFromText(?))';
const insertEntry = 'INSERT INTO entry ' + 
  '(entry_name,picture,restaurant_id,entry_date,rating,noodles_id,broth_id,toppings_id,notes) ' +
  'VALUES(?,?,?,STR_TO_DATE(?,"%d/%m/%Y"),?,?,?,?,?)';
const selectRestaurantByLatLng = 'SELECT restaurant_id FROM restaurant WHERE lat_lng = POINT(?,?)';

// TODO verify fields
// TODO promise-ify

function objectToArray(obj) {
  return Object.keys(obj).map(key => obj[key]);
}

function getRestaurantValues(restaurant) {
  const point = 'POINT(' + restaurant.lat + ' ' + restaurant.lng + ')';
  return [restaurant.restaurant_name, restaurant.website, restaurant.locality, 
    restaurant.administrative_area, restaurant.country, point];
}

function getAverageRating(broth, noodles, toppings) {
  return ((broth.rating + noodles.rating + toppings.rating) / 3).toFixed(1);
}

if (process.argv.length < 3) {
  console.log('File not specified');
  process.exit(1);
}

// Read in JSON file
let obj;
fs.readFile(process.argv[2], 'utf8', (error, data) => {
  if (error) {
    console.log(error);
    process.exit(1);
  }
  obj = JSON.parse(data);

  // Break into parts (broth, noodle, toppings, restaurant, entry)
  let broth = obj.broth;
  let noodles = obj.noodles;
  let toppings = obj.toppings;
  let restaurant = obj.restaurant;
  let entry = obj.entry;

  // Create connection to db
  const connection = mysql.createConnection(config.mysql);
  connection.connect();

  // Attempt to update db
  try {
    // Check lat_lng of restaurant to create 
    connection.query(selectRestaurantByLatLng, [restaurant.lat, restaurant.lng], (error, results, fields) => {
      if (error) {
        throw error;
      }
      // If restaurant already exists, get id of that restaurant
      if (results.length) {
        const restaurantId = results[0].restaurant_id;
        connection.beginTransaction(error => {
          if (error) {
            throw error;
          }
          // Create broth
          connection.query(insertBroth, objectToArray(broth), (error, results, fields) => {
            if (error) {
              return connection.rollback(() => {
                throw error;
              });
            }
            const brothId = results.insertId;

            // Create noodles
            connection.query(insertNoodles, objectToArray(noodles), (error, results, fields) => {
              if (error) {
                return connection.rollback(() => {
                  throw error;
                });
              }
              const noodlesId = results.insertId;

              // Create toppings
              connection.query(insertToppings, objectToArray(toppings), (error, results, fields) => {
                if (error) {
                  return connection.rollback(() => {
                    throw error;
                  });
                }
                const toppingsId = results.insertId;

                // Create entry
                const rating = getAverageRating(broth, noodles, toppings);
                const entryValues = [entry.entry_name, entry.picture, restaurantId, entry.entry_date, rating, 
                  noodlesId, brothId, toppingsId, entry.notes];
                connection.query(insertEntry, entryValues, (error, results, fields) => {
                  if (error) {
                    return connection.rollback(() => {
                      throw error;
                    });
                  }
                  connection.commit((error) => {
                    if (error) {
                      return connection.rollback(() => {
                        throw error;
                      });
                    }
                    process.exit();
                  });
                });
              });
            });
          });
        });
      } else {
        // Restaurant does not exist; create new restaurant and get id of created record
        connection.beginTransaction(error => {
          if (error) {
            throw error;
          }

          // Create restaurant
          console.log(connection.format(insertRestaurant, getRestaurantValues(restaurant)));
          connection.query(insertRestaurant, getRestaurantValues(restaurant), (error, results, fields) => {
            if (error) {
              return connection.rollback(() => {
                throw error;
              });
            }
            const restaurantId = results.insertId;

            // Create broth
            connection.query(insertBroth, objectToArray(broth), (error, results, fields) => {
              if (error) {
                return connection.rollback(() => {
                  throw error;
                });
              }
              const brothId = results.insertId;

              // Create noodles
              connection.query(insertNoodles, objectToArray(noodles), (error, results, fields) => {
                if (error) {
                  return connection.rollback(() => {
                    throw error;
                  });
                }
                const noodlesId = results.insertId;

                // Creating toppings
                connection.query(insertToppings, objectToArray(toppings), (error, results, fields) => {
                  if (error) {
                    return connection.rollback(() => {
                      throw error;
                    });
                  }
                  const toppingsId = results.insertId;

                  // Create entry
                  const rating = getAverageRating(broth, noodles, toppings);
                  const entryValues = [entry.entry_name, entry.picture, restaurantId, entry.entry_date, rating, 
                    noodlesId, brothId, toppingsId, entry.notes];
                  connection.query(insertEntry, entryValues, (error, results, fields) => {
                    if (error) {
                      return connection.rollback(() => {
                        throw error;
                      });
                    }
                    connection.commit((error) => {
                      if (error) {
                        return connection.rollback(() => {
                          throw error;
                        });
                      }
                      process.exit();
                    });
                  });
                });
              });
            });
          });
        });
      }
    });
  } catch (error) {
    console.log(error)
  } finally {
    //connection.end();
  }
});
