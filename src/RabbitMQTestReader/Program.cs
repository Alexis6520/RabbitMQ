using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var hostName = "10.10.50.28";
var connFactory = new ConnectionFactory
{
    HostName = hostName,
    ClientProvidedName = "RabbitMQReaderTestApp",
    AutomaticRecoveryEnabled = true,
};

//----------------- CONSUMIR UNA COLA-----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();
//var queueName = "hello";
//channel.QueueDeclare(queueName, true, false, false); // Para asegurarnos que la cola exista
//channel.BasicQos(0, 1, false); // Indica al servidor que no envíe un nuevo mensaje a este canal hasta que el anterior haya sido confirmado
//var consumer = new EventingBasicConsumer(channel);

//// Agregar eventos que se ejecutaran al recibir un mensaje de la cola
//consumer.Received += (model, args) =>
//{
//    var body = args.Body.ToArray(); // SIEMPRE COPIA LOS DATOS PORQUE PUEDEN LIBERARSE DE LA MEMORIA
//    var message = Encoding.UTF8.GetString(body);
//    Console.WriteLine(message);
//    channel.BasicAck(args.DeliveryTag, false);  // Confirma el mensaje de recibido
//};

//// Inicia el consumo de la cola
//channel.BasicConsume(queueName, false, consumer);
//Console.ReadLine();
//channel.Close();
//connection.Close();

//----------------- CONSUMIR UNA COLA VINCULADA A UN PROPAGADOR-----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();
//var queueName = channel.QueueDeclare().QueueName; // Crea una cola temporal que se eliminará automáticamente al cerrar esta conexión
//channel.BasicQos(0, 1, false); // Indica al servidor que no envíe un nuevo mensaje a este canal hasta que el anterior haya sido confirmado
//var exchangeName = "logs";
//channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout); // Se asegura que el propagador exista
//channel.QueueBind(queueName, exchangeName, string.Empty); // vincula la cola al propagador
//var consumer = new EventingBasicConsumer(channel);

//// Agregar eventos que se ejecutaran al recibir un mensaje de la cola
//consumer.Received += (model, args) =>
//{
//    var body = args.Body.ToArray(); // SIEMPRE COPIA LOS DATOS PORQUE PUEDEN LIBERARSE DE LA MEMORIA
//    var message = Encoding.UTF8.GetString(body);
//    Console.WriteLine(message);
//    channel.BasicAck(args.DeliveryTag, false);  // Confirma el mensaje de recibido
//};

//// Inicia el consumo de la cola
//channel.BasicConsume(queueName, false, consumer);
//Console.ReadLine();
//channel.Close();
//connection.Close();

//----------------- CONSUMIR UNA COLA VINCULADA A UN PROPAGADOR Y FILTRADA POR UN TEMA-----------------
using var connection = connFactory.CreateConnection();
using var channel = connection.CreateModel();
var queueName = channel.QueueDeclare().QueueName; // Crea una cola temporal que se eliminará automáticamente al cerrar esta conexión
channel.BasicQos(0, 1, false); // Indica al servidor que no envíe un nuevo mensaje a este canal hasta que el anterior haya sido confirmado
var exchangeName = "topic_logs";
channel.ExchangeDeclare(exchangeName, ExchangeType.Topic); // Se asegura que el propagador exista con el tipo TEMA
var topic = "*.orange.*"; // *-Una palabra cualquiera,#-Una o más palabras
channel.QueueBind(queueName, exchangeName, topic); // vincula la cola al propagador con la ruta(tema especificado). Se pueden hacer vinculaciones multiples con diferentes temas
var consumer = new EventingBasicConsumer(channel);

// Agregar eventos que se ejecutaran al recibir un mensaje de la cola
consumer.Received += (model, args) =>
{
    var body = args.Body.ToArray(); // SIEMPRE COPIA LOS DATOS PORQUE PUEDEN LIBERARSE DE LA MEMORIA
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine(message);
    channel.BasicAck(args.DeliveryTag, false);  // Confirma el mensaje de recibido
};

// Inicia el consumo de la cola
channel.BasicConsume(queueName, false, consumer);
Console.ReadLine();
channel.Close();
connection.Close();