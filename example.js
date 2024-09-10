const copy = require('./index').copy;

const globalAWSConfig = { // AWS Configuration object http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property
  // accessKeyId: 'AKID',
  // secretAccessKey: 'SECRET',
  region: 'ap-south-1'
};

const sourceAWSConfig = {
  accessKeyId: 'AKID',
  secretAccessKey: 'SECRET',
  region: 'eu-west-1'
};

const destinationAWSConfig = {
  accessKeyId: 'AKID',
  secretAccessKey: 'SECRET',
  region: 'us-west-2' // support cross zone copying
};

(async function() {
  await copy({
    config: globalAWSConfig,
    source: {
      tableName: 'vxsgProductRecos', // required
      // config: sourceAWSConfig // optional , leave blank to use globalAWSConfig
    },
    destination: {
      tableName: 'vxphTestProductRecos4', // required
      // config: destinationAWSConfig // optional , leave blank to use globalAWSConfig
    },
    log: true, // default false
    create: false, // create table if not exist
    schemaOnly: true, // make it true and it will copy schema only
    continuousBackups: true, // default 'copy', true will always enable backups, 'copy' copies the behaviour from the source and false does not enable them
    transformDataFn: function(item){ return item } // function to transform data
  });
})();


