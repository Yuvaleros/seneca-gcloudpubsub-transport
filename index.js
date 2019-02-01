const {PubSub} = require('@google-cloud/pubsub');
const _ = require('lodash');
const nid = require('nid');

module.exports = function (options) {
  const seneca = this;
  const plugin = 'gcloudpubsub-transport';
  let pubsub;

  const so = seneca.options();
  const transportUtils = seneca.export('transport/utils');

  options = seneca.util.deepextend({
      PubSub: {
      topicPrefix: '',
      projectId: '',
      keyFilename: ''
    }
  }, options);

  let topicPrefix = options.PubSub.topicPrefix;

  seneca.add('role:transport,hook:listen,type:gcloud', hook_listen_gcloud);
  seneca.add('role:transport,hook:client,type:gcloud', hook_client_gcloud);
  seneca.add('role:seneca,cmd:close', shutdown);

  function make_error_handler(type, tag) {
    return (note, err) => {
      seneca.log.error(type, tag, note, err, 'CLOSED');
    }
  }

  async function init(opts) {
    // seneca.log.info('OPTIONS');
    // seneca.log.info(options['gcloud']);
    // const clientOpts = JSON.parse(options['gcloud']);

    pubsub = new PubSub({
      projectId: opts.projectId,
      keyFilename: opts.keyFilename
    });
    topicPrefix = opts.topicPrefix;
    seneca.log.info('Connected to GCloud PubSub');
    // seneca.log.info(pubsub);
    return pubsub;
  }

  function createTopics(pubsub) {
    // Validate the topic prefix
    // seneca.log.info('OPTS: ' + opts);
    async function validatePrefix(topicPrefix) {
      if (!_.isString(topicPrefix) || _.isEmpty(topicPrefix)
        || topicPrefix.length > 250) {
        throw new Error('topicPrefix must be a valid string 250 characters or less!');
      }
    }

    // Create the request and response topics
    async function topicCreator(topicName) {
      try {
        const topic = pubsub.topic(topicName);
        const [existingTopic] = await topic.get({ autoCreate: true });
        seneca.log.info(`Topic "${topicName}" created (or it existed already)`);
        return existingTopic;
      } catch (err) {
        if (err.code === 409) { // If the topic already exists, just return it
          seneca.log.warn('Topic "' + topicName + '" already exists.');
          return pubsub.topic(topicName);
        }
        else {
          seneca.log.info('Failed to create topic: ', topicName);
          throw err;
        }
      }
    }

    return Promise.all([
      validatePrefix(topicPrefix),
      topicCreator(topicPrefix + '.act'),
      topicCreator(topicPrefix + '.res')
    ]).then(results => ({
      act: results[1],
      res: results[2]
    }));
  }

  // Subscribe to a topic object
  async function createSubscription(topic, kind) {
    const subscriber_name = topicPrefix + '.' + kind;
    const subs_options = {
      flowControl: {
        maxMessages: 1,
        reuseExisting: true
      }
    };

    try {
      const [subscription] = await topic.createSubscription(subscriber_name, subs_options);
      seneca.log.info(`Created subscription to "${topic.name}", Subscription: ${subscriber_name}`);
      return subscription;
    } catch (err) {
      seneca.log.error(`Failed to subscribe to "${topic.name}"`);
      throw err;
    }
  }

  function hook_listen_gcloud(args, done) {
    const type = args.type;
    const listen_options = seneca.util.clean(_.extend({}, options[type], args));
    topicPrefix = listen_options.topicPrefix;

    init(listen_options)
      .then(createTopics)
      .then(subscribeTopics)
      .then(() => {
        done();
      })
      .catch(done);

    async function subscribeTopics(topics) {
      const act_topic = topics.act; // The request topic
      const res_topic = topics.res; // The response topic

      const subscription = await createSubscription(act_topic, 'act');

      function onMessage(message) {
        // seneca.log.info('Got a request: ' + message.id);

        const content = JSON.parse(message.data.toString());
        const data = transportUtils.parseJSON(seneca, 'listen-' + type, content);

        // Publish message
        transportUtils.handle_request(seneca, data, listen_options, async out => {
          if (out == null) return;

          try {
            const publisher = res_topic.publisher();
            await publisher.publish(Buffer.from(JSON.stringify(transportUtils.stringifyJSON(seneca, 'listen-' + type, out))));
          } catch (err) {
            if (err)
              seneca.log.error('Failed to send message: ' + err);
          } finally {
            message.ack(err => {
              if (err)
                seneca.log.warn('Failed to ack message: ' + message.id);
            });
          }
        });
      }

      seneca.log.info('Subscribing to ' + subscription.name);
      subscription.on('message', onMessage);

    }
  }

  function hook_client_gcloud(args, client_done) {
    const seneca = this;
    const type = args.type;
    const client_options = seneca.util.clean(_.extend({}, options[type], args));

    init(client_options)
      .then(createTopics)
      .then(subscribeTopics)
      .then(createClient)
      .then(client => {
        client_done(null, client);
      })
      .catch(err => {
        client_done(err);
      });

    async function subscribeTopics(topics) {
      const res_topic = topics.res; // The response topic

      const subscription = await createSubscription(res_topic, 'res');
      return {
        topics: topics,
        subscription
      };
    }

    async function createClient(params) {
      const act_topic = params.topics.act;
      const subscription = params.subscription;

      // Subscribe to the response topic
      seneca.log.info('Subscribing to ' + subscription.name);
      subscription.on('message', onMessage);

      function onMessage(message) {
        // seneca.log.info('Got a response: ' + message.id);

        const content = JSON.parse(message.data.toString());
        const input = transportUtils.parseJSON(seneca, 'client-' + type, content);

        // Acknowledge the message
        message.ack(err => {
          if (err)
            seneca.log.warn('Failed to ack message: ' + message.id);
        });
        transportUtils.handle_response(seneca, input, client_options);
      }

      return {
        id: nid(),
        toString: function () {
          return 'any-' + this.id;
        },

        // TODO: is this used?
        match: function (args) {
          return !this.has(args);
        },

        send: async function (args, done, meta) {
          const outmsg = transportUtils.prepare_request(this, args, done, meta);

          try {
            const publisher = pubsub.topic(act_topic.name).publisher();
            publisher.publish('Hello');
            await publisher.publish(Buffer.from(JSON.stringify(transportUtils.stringifyJSON(seneca, 'client-' + type, outmsg))));
          } catch (err) {
            seneca.log.error('Failed to send message: ' + err);
          }
        }
      };
    }
  }

  function shutdown(args, done) {
    done();
  }

  return {
    name: plugin
  };
};
