using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;

namespace RendererReceiver
{
    class Program
    {
        const string ServiceBusConnectionString = "<service_bus_connection_string_here>";
        const string TopicName = "session-test";
        const string SubscriptionName = "session-sub";
        static ISubscriptionClient subscriptionClient;

        public static async Task Main(string[] args)
        {
            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

            await ConfigureServiceBusAsync();

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register subscription message handler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();

            await subscriptionClient.CloseAsync();
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
            subscriptionClient.RegisterSessionHandler(ProcessSessionMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessSessionMessagesAsync(IMessageSession session, Message message, CancellationToken token)
        {
            dynamic msg = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(message.Body));

            lock (Console.Out)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(
                    "Message received:  \n\tSessionId = {0}, \n\tMessageId = {1}, \n\tSequenceNumber = {2}," +
                    "\n\tContent: [ Name = {3}, Step = {4} ]",
                    message.SessionId,
                    message.MessageId,
                    message.SystemProperties.SequenceNumber,
                    msg.name,
                    msg.step);
                Console.ResetColor();
            }

            await SendCompletionAsync(message.ReplyTo, message.ReplyToSessionId, msg);

            await session.CompleteAsync(message.SystemProperties.LockToken);

            if (msg.step == 5)
            {
                // end of the session!
                await session.CloseAsync();
                Console.WriteLine("\n\t********** END OF SESSION *************");
            }
        }

        static async Task SendCompletionAsync(string topic, string sessionId, dynamic msg)
        {
            // Need some sort of factory here to create this as topic could vary message to message
            var topicClient = new TopicClient(ServiceBusConnectionString, topic);

            dynamic data = new {status = "success", msg.name, msg.step};

            try
            {
                // Create a new message to send to the topic.
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data)))
                {
                    ContentType = "application/json",
                    Label = "Status",
                    TimeToLive = TimeSpan.FromMinutes(2),
                    SessionId = sessionId
                };

                // Write the body of the message to the console.
                Console.WriteLine($"Sending completion message for: {msg.name}");

                // Send the message to the topic.
                await topicClient.SendAsync(message);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        private static async Task ConfigureServiceBusAsync()
        {
            var mgmtClient = new ManagementClient(ServiceBusConnectionString);

            // Create our topic
            if (!await mgmtClient.TopicExistsAsync(TopicName))
                await mgmtClient.CreateTopicAsync(new TopicDescription(TopicName) { EnableBatchedOperations = true });

            // Create the subscription we need
            if (!await mgmtClient.SubscriptionExistsAsync(TopicName, SubscriptionName))
            {
                await mgmtClient.CreateSubscriptionAsync(new SubscriptionDescription(TopicName, SubscriptionName) { RequiresSession = true, MaxDeliveryCount = 3 });
                await subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName);
                await subscriptionClient.AddRuleAsync(new RuleDescription { Filter = new CorrelationFilter { Label = "Renderer" }, Name = SubscriptionName });
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
