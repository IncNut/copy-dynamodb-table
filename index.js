'use strict'
const { DynamoDBClient, UpdateContinuousBackupsCommand, DescribeContinuousBackupsCommand, DescribeTableCommand, ScanCommand, BatchWriteItemCommand, CreateTableCommand } = require("@aws-sdk/client-dynamodb");
const validate = require('./validate');
const readline = require('readline');

async function copy(values) {

  try {
    validate.config(values);
    console.log("Validation passed");
  } catch (err) {
   console.log(err);
  }

  const options = {
    config: values.config,
    source: {
      tableName: values.source.tableName,
      dynamoClient: values.source.dynamoClient || new DynamoDBClient(values.source.config || values.config)
    },
    destination: {
      tableName: values.destination.tableName,
      dynamoClient: values.destination.dynamoClient || new DynamoDBClient(values.destination.config || values.config),
      createTableStr: `Creating Destination Table (${values.destination.tableName}) `
    },
    key: values.key,
    counter: values.counter || 0,
    retries: 0,
    data: {},
    transform: values.transform,
    log: values.log,
    create: values.create,
    schemaOnly: values.schemaOnly,
    continuousBackups: values.continuousBackups
  };

  if (options.create) {
    const describeCommand = new DescribeTableCommand({ TableName: options.source.tableName });
    const resp = await options.source.dynamoClient.send(describeCommand);
    options.source.active = resp.Table.TableStatus === 'ACTIVE';
    resp.Table.TableName = options.destination.tableName;
    const createCommand = new CreateTableCommand(clearTableSchema(resp.Table));
    await options.destination.dynamoClient.send(createCommand);
    // console.log("Waiting for destination table to be active");
    await waitForActive(options);
    if (options.continuousBackups === "copy") { // copy backup options
      await setContinuousBackups(options);
    } else if(options.continuousBackups) {
      await enableBackups(options);
    }
  }

  await checkTables(options);
  await startCopying(options);
}

function enableBackups(options) {
  console.log("Enabling backups");
  const command = new UpdateContinuousBackupsCommand({
    PointInTimeRecoverySpecification: {
      PointInTimeRecoveryEnabled: true
    },
    TableName: options.destination.tableName
  });
  return options.destination.dynamoClient.send(command);
}

async function setContinuousBackups(options) {
  const command = new DescribeContinuousBackupsCommand({ TableName: options.source.tableName });
  const data = await options.source.dynamoClient.send(command);
  if (data.ContinuousBackupsDescription.ContinuousBackupsStatus === 'ENABLED') {
    return enableBackups(options);
  }
}

function clearTableSchema(table) {

  delete table.TableStatus;
  delete table.CreationDateTime;
  if (table.ProvisionedThroughput.ReadCapacityUnits === 0 && table.ProvisionedThroughput.WriteCapacityUnits === 0) {
    delete table.ProvisionedThroughput
  }
  else {
    delete table.ProvisionedThroughput.LastIncreaseDateTime;
    delete table.ProvisionedThroughput.LastDecreaseDateTime;
    delete table.ProvisionedThroughput.NumberOfDecreasesToday;
  }

  delete table.TableSizeBytes;
  delete table.ItemCount;
  delete table.TableArn;
  delete table.TableId;
  delete table.LatestStreamLabel;
  delete table.LatestStreamArn;

  if (table.LocalSecondaryIndexes && table.LocalSecondaryIndexes.length > 0) {
    for (let i = 0; i < table.LocalSecondaryIndexes.length; i++) {
      delete table.LocalSecondaryIndexes[i].IndexStatus;
      delete table.LocalSecondaryIndexes[i].IndexSizeBytes;
      delete table.LocalSecondaryIndexes[i].ItemCount;
      delete table.LocalSecondaryIndexes[i].IndexArn;
      delete table.LocalSecondaryIndexes[i].LatestStreamLabel;
      delete table.LocalSecondaryIndexes[i].LatestStreamArn;
    }
  }


  if (table.GlobalSecondaryIndexes && table.GlobalSecondaryIndexes.length > 0) {
    for (let j = 0; j < table.GlobalSecondaryIndexes.length; j++) {
      delete table.GlobalSecondaryIndexes[j].IndexStatus;
      if (table.GlobalSecondaryIndexes[j].ProvisionedThroughput.ReadCapacityUnits === 0 && table.GlobalSecondaryIndexes[j].ProvisionedThroughput.WriteCapacityUnits === 0) {
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput
      }
      else {
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput.LastIncreaseDateTime;
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput.LastDecreaseDateTime;
        delete table.GlobalSecondaryIndexes[j].ProvisionedThroughput.NumberOfDecreasesToday;
      }
      delete table.GlobalSecondaryIndexes[j].IndexSizeBytes;
      delete table.GlobalSecondaryIndexes[j].ItemCount;
      delete table.GlobalSecondaryIndexes[j].IndexArn;
      delete table.GlobalSecondaryIndexes[j].LatestStreamLabel;
      delete table.GlobalSecondaryIndexes[j].LatestStreamArn;
    }
  }

  if (table.SSEDescription) {
    table.SSESpecification = {
      Enabled: (table.SSEDescription.Status === 'ENABLED' || table.SSEDescription.Status === 'ENABLING'),
    };
    delete table.SSEDescription;
  }

  if (table.BillingModeSummary) {
    table.BillingMode = table.BillingModeSummary.BillingMode
  }
  delete table.BillingModeSummary;
  return table;
}


async function checkTables(options) {
  const sourceCommand = new DescribeTableCommand({ TableName: options.source.tableName });
  const sourceData = await options.source.dynamoClient.send(sourceCommand);
  if (sourceData.Table.TableStatus !== 'ACTIVE') {
    throw new Error('Source table not active');
  }
  const destCommand = new DescribeTableCommand({ TableName: options.destination.tableName });
  const destData = await options.destination.dynamoClient.send(destCommand);
  if (destData.Table.TableStatus !== 'ACTIVE') {
    throw new Error('Destination table not active');
  }
}

function waitForActive(options) {
  return new Promise(function (resolve) {
    (async function checkTableStatus() {
      const command = new DescribeTableCommand({TableName: options.destination.tableName});
      const data = await options.destination.dynamoClient.send(command);
      if (options.log) {
        options.destination.createTableStr += '.'
        readline.clearLine(process.stdout)
        readline.cursorTo(process.stdout, 0)
        process.stdout.write(options.destination.createTableStr)
      }
      if (data.Table.TableStatus !== 'ACTIVE') { // wait for active
        setTimeout(checkTableStatus, 1000);
      } else {
        return resolve();
      }
    })();
  });
}

async function startCopying(options, fn) {
  while(true) {
    const data = await scan(options);
    options.key = data.LastEvaluatedKey;
    const items = mapItems(options, data);
    await putItems(options, items);
    if (options.log) {
      readline.clearLine(process.stdout)
      readline.cursorTo(process.stdout, 0)
      process.stdout.write('Copied ' + options.counter + ' items')
    }
    if (options.key === undefined) {
      break;
    }
  }
}



async function scan(options) {
  const command = new ScanCommand({
    TableName: options.source.tableName,
    Limit: 5,
    ExclusiveStartKey: options.key
  });
  return options.source.dynamoClient.send(command);
}

function mapItems(options, data) {
  return data.Items.map(function (item, index) {
    return {
      PutRequest: {
        Item: !!options.transform ? options.transform(item, index) : item
      }
    }
  });
}

async function putItems(options, items) {
  if (!items || items.length === 0) {
    return null;
  }
  const batchWriteItems = {};
  batchWriteItems.RequestItems = {}
  batchWriteItems.RequestItems[options.destination.tableName] = items;
  const command = new BatchWriteItemCommand(batchWriteItems);
  const data = await options.destination.dynamoClient.send(command);
  const unprocessedItems = data.UnprocessedItems[options.destination.tableName];
  if (unprocessedItems !== undefined) {
    options.retries++
    options.counter += (items.length - unprocessedItems.length);
    return setTimeout(async function () {
      await putItems(options, unprocessedItems);
    }, 2 * options.retries * 100) // from aws http://docs.aws.amazon.com/general/latest/gr/api-retries.html
  }
  options.retries = 0;
  options.counter += items.length;
}

module.exports.copy = copy
