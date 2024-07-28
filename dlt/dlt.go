package dlt

import (
	"crypto/x509"
	"fmt"
	"os"
	"path"
	"reconciliation/model"
	"time"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// queryHyperledgerData queries the data from Hyperledger Fabric

type MatchedTrans struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	Transdatetime time.Time `json:"transdatetime"`
	Uetr          string    `json:"uetr"`
}

func QueryTrans(config *model.Config, chaincodeName string, startDate string, endDate string, docType string) ([]byte, error) {

	clientConnection := NewGrpcConnection(config)
	defer clientConnection.Close()

	id := NewIdentity(config)
	sign := NewSign(config)

	// Create a Gateway connection for a specific client identity
	gw, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithClientConnection(clientConnection),
		// Default timeouts for different gRPC calls
		client.WithEvaluateTimeout(5*time.Second),
		client.WithEndorseTimeout(15*time.Second),
		client.WithSubmitTimeout(5*time.Second),
		client.WithCommitStatusTimeout(1*time.Minute),
	)
	if err != nil {
		panic(err)
	}
	defer gw.Close()

	network := gw.GetNetwork(config.Dlt.ChannelID)
	contract := network.GetContract(config.Dlt.ChaincodeID)

	// Query the ledger
	queryResult, err := contract.EvaluateTransaction(chaincodeName, startDate, endDate, docType)
	if err != nil {
		panic(err)
		//return nil, err
	}

	fmt.Printf("*** Transaction committed successfully\n")

	return queryResult, nil

}

//func QueryHyperledgerData() ([]MatchedTrans, error) {
// Initialize the SDK
/*sdk, err := fabsdk.New(config.FromFile("path/to/your/config.yaml"))
if err != nil {
	return nil, fmt.Errorf("failed to create SDK: %v", err)
}
defer sdk.Close()

// Create the channel client
clientChannelContext := sdk.ChannelContext("mychannel", fabsdk.WithUser("Admin"))
client, err := channel.New(clientChannelContext)
if err != nil {
	return nil, fmt.Errorf("failed to create channel client: %v", err)
}

// Query the chaincode
response, err := client.Query(channel.Request{ChaincodeID: "mycc", Fcn: "queryAllUsers", Args: [][]byte{}})
if err != nil {
	return nil, fmt.Errorf("failed to query chaincode: %v", err)
}

var trans []MatchedTrans
if err := json.Unmarshal(response.Payload, &trans); err != nil {
	return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
}*/

/*clientConnection := newGrpcConnection(config)
	defer clientConnection.Close()

	id := newIdentity(config)
	sign := newSign(config)

	// Create a Gateway connection for a specific client identity
	gw, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithClientConnection(clientConnection),
		// Default timeouts for different gRPC calls
		client.WithEvaluateTimeout(5*time.Second),
		client.WithEndorseTimeout(15*time.Second),
		client.WithSubmitTimeout(5*time.Second),
		client.WithCommitStatusTimeout(1*time.Minute),
	)
	if err != nil {
		panic(err)
	}
	defer gw.Close()

	// Override default values for chaincode and channel name as they may differ in testing contexts.

	network := gw.GetNetwork(config.Dlt.ChannelID)
	contract := network.GetContract(config.Dlt.ChaincodeID)

	return trans, nil
}*/

// newGrpcConnection creates a gRPC connection to the Gateway server.
func NewGrpcConnection(config *model.Config) *grpc.ClientConn {

	certificatePEM, err := os.ReadFile(config.Fabric.CryptoPath + config.Fabric.TlsCertPath)
	if err != nil {
		panic(fmt.Errorf("failed to read TLS certifcate file: %w", err))
	}

	certificate, err := identity.CertificateFromPEM(certificatePEM)
	if err != nil {
		panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AddCert(certificate)
	transportCredentials := credentials.NewClientTLSFromCert(certPool, config.Fabric.GatewayPeer)

	connection, err := grpc.NewClient(config.Fabric.PeerEndpoint, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		panic(fmt.Errorf("failed to create gRPC connection: %w", err))
	}

	//defer connection.Close()

	return connection
}

func GetGrpcContract(config *model.Config, connection *grpc.ClientConn) *client.Gateway {
	id := NewIdentity(config)
	sign := NewSign(config)

	// Create a Gateway connection for a specific client identity
	gw, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithClientConnection(connection),
		// Default timeouts for different gRPC calls
		client.WithEvaluateTimeout(5*time.Second),
		client.WithEndorseTimeout(15*time.Second),
		client.WithSubmitTimeout(5*time.Second),
		client.WithCommitStatusTimeout(1*time.Minute),
	)
	if err != nil {
		panic(err)
	}
	defer gw.Close()

	// Override default values for chaincode and channel name as they may differ in testing contexts.

	return gw

}

// newIdentity creates a client identity for this Gateway connection using an X.509 certificate.
func NewIdentity(config *model.Config) *identity.X509Identity {
	certificatePEM, err := readFirstFile(config.Fabric.CryptoPath + config.Fabric.CertPath)
	if err != nil {
		panic(fmt.Errorf("failed to read certificate file: %w", err))
	}

	certificate, err := identity.CertificateFromPEM(certificatePEM)
	if err != nil {
		panic(err)
	}

	id, err := identity.NewX509Identity(config.Fabric.MspID, certificate)
	if err != nil {
		panic(err)
	}

	return id
}

func readFirstFile(dirPath string) ([]byte, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}

	fileNames, err := dir.Readdirnames(1)
	if err != nil {
		return nil, err
	}

	return os.ReadFile(path.Join(dirPath, fileNames[0]))
}

// newSign creates a function that generates a digital signature from a message digest using a private key.
func NewSign(config *model.Config) identity.Sign {
	privateKeyPEM, err := readFirstFile(config.Fabric.CryptoPath + config.Fabric.KeyPath)
	if err != nil {
		panic(fmt.Errorf("failed to read private key file: %w", err))
	}

	privateKey, err := identity.PrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		panic(err)
	}

	sign, err := identity.NewPrivateKeySign(privateKey)
	if err != nil {
		panic(err)
	}

	return sign
}

func UpdateTrans(config *model.Config, DLTId string, updt string) ([]byte, error) {

	clientConnection := NewGrpcConnection(config)
	defer clientConnection.Close()

	id := NewIdentity(config)
	sign := NewSign(config)

	// Create a Gateway connection for a specific client identity
	gw, err := client.Connect(
		id,
		client.WithSign(sign),
		client.WithClientConnection(clientConnection),
		// Default timeouts for different gRPC calls
		client.WithEvaluateTimeout(5*time.Second),
		client.WithEndorseTimeout(15*time.Second),
		client.WithSubmitTimeout(5*time.Second),
		client.WithCommitStatusTimeout(1*time.Minute),
	)
	if err != nil {
		return nil, err
	}
	defer gw.Close()

	network := gw.GetNetwork(config.Dlt.ChannelID)
	contract := network.GetContract(config.Dlt.ChaincodeID)

	fmt.Printf("*** DLTId to be updated%v", DLTId)

	// Query the ledger
	queryResult, err := contract.SubmitTransaction("UpdateTrans", DLTId, updt)

	if err != nil {

		return nil, err
	}

	fmt.Printf("*** Transaction committed successfully\n")

	return queryResult, nil

}
