using RabbitMQ.Client;
using RabbitMQ.Model;
using System.Text.Json;

const string exchangeName = "pedido.exchange";
const string queueName = "pedido.queue";
const string routingKey = "pedido.criado";

//Conexão com o RabbitMQ
var factory = new RabbitMQ.Client.ConnectionFactory()
{
    HostName = "localhost",
    Port = 5672, //Padrão de protocolo AMQP
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    AutomaticRecoveryEnabled = true, //Se a conexão cair por algum motivo, o RabbitMQ tentará se reconectar automaticamente;
    NetworkRecoveryInterval = TimeSpan.FromSeconds(10) //Quantidade de tempo que o RabbitMQ aguardará antes de tentar se reconectar;
};

await using var connction = await factory.CreateConnectionAsync(); //Criar conexões é caro, então é recomendado criar uma conexão por aplicação;
await using var channel = await connction.CreateChannelAsync(); //Criar canais sai mais barato do que criar conexões, então é recomendado criar um canal por thread de execução;

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,//Nome do exchange que será declarado;
    type: RabbitMQ.Client.ExchangeType.Direct, //Tipo de exchange que será utilizado, nesse caso, um exchange do tipo "direct", que envia mensagens para filas com base na chave de roteamento (routing key);
    durable: true, //O exchange será persistido no disco, mesmo que o RabbitMQ seja reiniciado (NECESSÁRIO EM PRODUÇÃO);
    autoDelete: false,//Declarar o exchange, caso ele não exista. A opção autoDelete indica que o exchange não será deletado automaticamente quando não houver mais filas vinculadas a ele;
    arguments: null);

await channel.QueueDeclareAsync(
    queue: queueName, //Nome da fila que será declarada;
    durable: true, //A fila será persistida no disco, mesmo que o RabbitMQ seja reiniciado (NECESSÁRIO EM PRODUÇÃO);
    exclusive: false, //A fila não será exclusiva para a conexão atual, permitindo que outras conexões possam consumir mensagens dela;
    autoDelete: false, //A fila não será deletada automaticamente quando não houver mais consumidores;
    arguments: null);

//Sem o binding, a fila não receberá mensagens do exchange, mesmo que a chave de roteamento seja a mesma;
await channel.QueueBindAsync(
    queue: queueName, //Nome da fila que será vinculada ao exchange;
    exchange: exchangeName, //Nome do exchange que será vinculado à fila;
    routingKey: routingKey, //Chave de roteamento que será utilizada para enviar mensagens para a fila;
    arguments: null);

await channel.BasicQosAsync(
    prefetchSize: 0, //O tamanho máximo de mensagens que o consumidor pode receber antes de enviar um ack (acknowledgment) para o RabbitMQ. Nesse caso, 0 significa que não há limite de tamanho;
    prefetchCount: 1, //O número máximo de mensagens que o consumidor pode receber antes de enviar um ack (acknowledgment) para o RabbitMQ. Nesse caso, 1 significa que o consumidor só receberá uma mensagem por vez, evitando sobrecarga de processamento;
    global: false); //Indica se a configuração de QoS (Quality of Service) será aplicada globalmente para todos os consumidores do canal ou apenas para o consumidor atual. Nesse caso, false significa que a configuração será aplicada apenas para o consumidor atual;

var consumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel); //Criar um consumidor assíncrono para receber mensagens da fila;

consumer.ReceivedAsync += async (sender, eventArgs) =>
{
    try
    {
        var body = eventArgs.Body.ToArray(); //Obter o corpo da mensagem recebida;
        var json = System.Text.Encoding.UTF8.GetString(body); //Converter o corpo da mensagem de bytes para string;
        var pedido = JsonSerializer.Deserialize<Pedido>(json); //Desserializar a string JSON para um objeto do tipo JsonObject;
        Console.WriteLine($"Mensagem recebida: {json}"); //Exibir a mensagem recebida no console;
        Console.WriteLine($"Cliente: {pedido?.ClienteEmail}"); //Exibir a mensagem recebida no console;
        Console.WriteLine($"Valor: {pedido?.ValorTotal:c}"); //Exibir a mensagem recebida no console;
        Console.WriteLine($"Criado: {pedido?.DataCriacao:o}"); //Exibir a mensagem recebida no console;

        await Task.Delay(2000); //Simular um processamento, aguardando 2 segundos; Mandar E-mail, salvar no banco de dados, etc....;

        //Confirma o processamento da mensagem;
        await channel.BasicAckAsync(
            deliveryTag: eventArgs.DeliveryTag, //Identificador único da mensagem que foi recebida;
            multiple: false); //Indica se o ack (acknowledgment) será enviado para todas as mensagens não confirmadas ou apenas para a mensagem atual. Nesse caso, false significa que o ack será enviado apenas para a mensagem atual;
    }
    catch (JsonException je)
    {
        Console.WriteLine($"Erro ao desserializar a mensagem: {je.Message}");

        //Nack é uma negativação da mensagefm, ou seja, o consumidor está informando ao RabbitMQ que não conseguiu processar a mensagem e que ela deve ser descartada ou reenfileirada;
        await channel.BasicNackAsync(
            deliveryTag: eventArgs.DeliveryTag, //Identificador único da mensagem que foi recebida;
            multiple: false, //Indica se o nack (negative acknowledgment) será enviado para todas as mensagens não confirmadas ou apenas para a mensagem atual. Nesse caso, false significa que o nack será enviado apenas para a mensagem atual;
            requeue: false); //Indica se a mensagem será reenfileirada na fila ou descartada. Nesse caso, false significa que a mensagem será descartada;
        throw;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Erro ao processar a mensagem: {ex.Message}");
        await channel.BasicNackAsync(
            deliveryTag: eventArgs.DeliveryTag, //Identificador único da mensagem que foi recebida;
            multiple: false, //Indica se o nack (negative acknowledgment) será enviado para todas as mensagens não confirmadas ou apenas para a mensagem atual. Nesse caso, false significa que o nack será enviado apenas para a mensagem atual;
            requeue: true); //Indica se a mensagem será reenfileirada na fila ou descartada. Nesse caso, true significa que a mensagem será reenfileirada na fila;
        throw;
    }
};

await channel.BasicConsumeAsync(
      queue: queueName, //Nome da fila que será consumida;
      autoAck: false, //Indica se o consumidor enviará automaticamente um ack (acknowledgment) para o RabbitMQ após processar a mensagem. Nesse caso, false significa que o consumidor enviará manualmente o ack após processar a mensagem;
      consumer: consumer); //Consumidor que será utilizado para receber mensagens da fila;

Console.WriteLine("Aguardando mensagens... Pressione [enter] para sair.");
Console.ReadLine();