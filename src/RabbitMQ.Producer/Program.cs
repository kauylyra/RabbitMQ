using RabbitMQ.Client;
using RabbitMQ.Model;

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

Console.WriteLine("Quantos pedidos deseja criar?");
if(!int.TryParse(Console.ReadLine(), out int quantidadePedidos))
{
    quantidadePedidos = 3; //Valor padrão caso a entrada seja inválida
}

for(int i = 0; i < quantidadePedidos; i++)
{
    var pedido = CriarPedidoFake(i);
    var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(pedido);
    var properties = new BasicProperties
    {
        Persistent = true, //Mensagem persistente, mesmo que o RabbitMQ seja reiniciado (NECESSÁRIO EM PRODUÇÃO);
        ContentType = "application/json",
        ContentEncoding = "utf-8"
    };

    //Publica a mensagem no exchange, que será roteada para a fila vinculada com a chave de roteamento especificada;
    await channel.BasicPublishAsync(
        exchange: exchangeName, //Nome do exchange que será utilizado para enviar a mensagem;
        routingKey: routingKey, //Chave de roteamento que será utilizada para enviar a mensagem para a fila vinculada ao exchange;
        mandatory: false,   //Se a mensagem não puder ser roteada para nenhuma fila, ela será descartada (NECESSÁRIO EM PRODUÇÃO);
        basicProperties: properties, //Propriedades da mensagem, como persistência, tipo de conteúdo e codificação;
        body: body //Corpo da mensagem, que será enviado para a fila vinculada ao exchange
    );

    Console.WriteLine($"Pedido {pedido.Id} enviado com sucesso!");

    Console.WriteLine($"Pressione qualquer tecla para continuar...");
    Console.ReadLine();
}

static Pedido CriarPedidoFake(int index)
{
    return new Pedido
    {
        Id = Guid.NewGuid(),
        ClienteEmail = $"cliente{index}@exemplo.com",
        ValorTotal = Random.Shared.Next(100, 1000),
        DataCriacao = DateTime.Now,
        Itens = new List<Item>
        {
            new Item
            {
                NomeProduto = $"Produto {index}",
                Quantidade = Random.Shared.Next(1, 10),
                PrecoUnitario = Random.Shared.Next(10, 100)
            }
        }
    };
}

Console.WriteLine("Fila e exchange declarados com sucesso!");