using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;

namespace ServiceBusSender
{
    class Program
    {
        const string ServiceBusConnectionString = "<service_bus_connection_string_here>";
        const string TopicName = "session-test";
        const string StatusSubscription = "session-status";
        static ITopicClient topicClient;
        static ISubscriptionClient statusSubscriptionClient;

        public static async Task Main(string[] args)
        {
            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            // Client to look out for status messages
            statusSubscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, StatusSubscription);

            // Create everything we need
            await ConfigureServiceBusAsync();

            // Register subscription message handler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            // Send messages.
            await Task.WhenAll(
                SendMessagesAsync(Guid.NewGuid().ToString(), "Renderer"),
                SendMessagesAsync(Guid.NewGuid().ToString(), "Renderer"),
                SendMessagesAsync(Guid.NewGuid().ToString(), "Renderer"),
                SendMessagesAsync(Guid.NewGuid().ToString(), "Renderer")
            );

            Console.ReadKey();

            await topicClient.CloseAsync();
        }

        static async Task SendMessagesAsync(string sessionId, string messageType)
        {
            dynamic data = new[]
            {
                new {externalId = Guid.NewGuid(), name = "Paul Gales", step = 1},
                new {externalId = Guid.NewGuid(), name = "John Smith", step = 2},
                new {externalId = Guid.NewGuid(), name = "Jane Does", step = 3},
                new {externalId = Guid.NewGuid(), name = "Testing Smith", step = 4},
                new {externalId = Guid.NewGuid(), name = "Testing Jones", step = 5},
            };

            try
            {
                for (var i = 0; i < data.Length; i++)
                {
                    // Create a new message to send to the topic.
                    var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i])))
                    {
                        SessionId = sessionId,
                        ContentType = "application/json",
                        Label = messageType,
                        MessageId = i.ToString(),
                        TimeToLive = TimeSpan.FromMinutes(2),
                        ReplyTo = TopicName,
                        ReplyToSessionId = sessionId
                    };

                    // Write the body of the message to the console.
                    Console.WriteLine($"Sending message: {messageType}:{i}");

                    // Send the message to the topic.
                    await topicClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new SessionHandlerOptions(ExceptionReceivedHandler)
            {
                MaxAutoRenewDuration = TimeSpan.FromSeconds(5),

                // Maximum number of concurrent sessions to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentSessions = 1,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };

            // Register the function that processes messages.
            statusSubscriptionClient.RegisterSessionHandler(ProcessStatusMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessStatusMessagesAsync(IMessageSession session, Message message, CancellationToken token)
        {
            dynamic msg = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(message.Body));

            lock (Console.Out)
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine(
                    "Status message received:" +
                    "\n\tContent: [ Name = {0}, status = {1} ]",
                    msg.name,
                    msg.status);
                Console.ResetColor();
            }

            await session.CompleteAsync(message.SystemProperties.LockToken);

            if (msg.step == 5)
            {
                // end of the session!
                await session.CloseAsync();
                Console.WriteLine("\n\t********** END OF SESSION *************");
            }
        }

        private static async Task ConfigureServiceBusAsync()
        {
            var mgmtClient = new ManagementClient(ServiceBusConnectionString);

            // Create our topic
            if (!await mgmtClient.TopicExistsAsync(TopicName))
                await mgmtClient.CreateTopicAsync(new TopicDescription(TopicName) { EnableBatchedOperations = true });

            // Create the subscription we need
            if (!await mgmtClient.SubscriptionExistsAsync(TopicName, StatusSubscription))
            {
                await mgmtClient.CreateSubscriptionAsync(new SubscriptionDescription(TopicName, StatusSubscription) { RequiresSession = true, MaxDeliveryCount = 3 });
                await statusSubscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName);
                var filter = new CorrelationFilter
                {
                    Label = "Status"
                };
                await statusSubscriptionClient.AddRuleAsync(new RuleDescription(StatusSubscription, filter));
            }
        }

        // Use this handler to examine the exceptions received on the message pump.
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}