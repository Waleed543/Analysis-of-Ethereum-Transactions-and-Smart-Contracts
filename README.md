
# Analysis of Ethereum Transactions and Smart Contracts using Apache Spark
The goal of this coursework is to apply the techniques covered in the first half of Big Data Processing to analyse the full set of transactions which have occurred on the Ethereum network; from the first transactions in August 2015 till January 2019. You will create several Map/Reduce or Spark programs to perform multiple types of computation. You will submit a report containing your results alongside an explanation of how they were obtained.
There are many resources available for understanding Ethereum and blockchain technology; a good place to start are these two short videos taken from this article, followed up by the Ethereum White Paper . There are also may sites dedicated to providing information about individual blocks, transactions and wallets, such as Etherscan and Ethplorer.

## Dataset overview
Ethereum is a blockchain based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mint their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralised, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

Whilst you would normally need a CLI tool such as GETH to access the Ethereum blockchain, recent tools allow scraping all block/transactions and dump these to csv's to be processed in bulk; notably Ethereum-ETL. These dumps are uploaded daily into a repository on Google BigQuery.

A subset of the Ethereum data is provided on the data repository bucket /data-repository-bkt/ECS765/ethereum-parvulus. We have also downloaded a set of scams, both active and inactive, run on the Ethereum network via etherscamDB which is available in the same folder.

## Dataset Schema - blocks
number: The block number

hash: Hash of the block

parent_hash: Hash of the parent of the block

nonce: Nonce that satisfies the difficulty target

sha3_uncles: Combined has of all uncles for a given parent

logs_bloom: Data structure containing event logs

transactions_root: Root hash of the transactions in the payload

state_root: Root hash of the state object

receipts_root: hash of the transaction receipts tree

miner: The address of the beneficiary to whom the mining rewards were given

difficulty: Integer of the difficulty for this block

total_difficulty: Total difficulty of the chain until this block

size: The size of this block in bytes

extra_data: Arbitrary additional data as raw bytes

gas_limit: The maximum gas allowed in this block

gas_used: The total used gas by all transactions in this block

timestamp: The timestamp for when the block was collated

transaction_count: The number of transactions in the block

base_fee_per_gas: Base fee value

## Dataset Schema - transactions
hash: Hash of the block

nonce: Nonce that satisfies the difficulty target

block_hash: Hash of the block where the transaction is in

block_number: Block number where this transaction was in

transaction_index: Transactions index position in the block.

from_address: Address of the sender

to_address: Address of the receiver. null when it is a contract creation transaction

value: Value transferred in Wei (the smallest denomination of ether)

gas: Gas provided by the sender

gas_price : Gas price provided by the sender in Wei

input: Extra data for Ethereum functions

block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

max_fee_per_gas: Sum of base fee and max priority fee

max_priority_fee_per_gas: Tip for mining the transaction

transaction_type: Value used to indicate if the transaction is related to a contract or other specialised transaction

## Dataset Schema - contracts
address: Address of the contract

bytecode: Code for Ethereum Contract

function_sighashes: Function signature hashes of a contract

is_erc20: Whether this contract is an ERC20 contract

is_erc721: Whether this contract is an ERC721 contract

block_number: Block number where this contract was created

## Dataset Schema - scams.json
id: Unique ID for the reported scam

name: Name of the Scam

url: Hosting URL

coin: Currency the scam is attempting to gain

category: Category of scam - Phishing, Ransomware, Trust Trade, etc.

subcategory: Subdivisions of Category

description: Description of the scam provided by the reporter and datasource

addresses: List of known addresses associated with the scam

reporter: User/company who reported the scam first

ip: IP address of the reporter

status: If the scam is currently active, inactive or has been taken offline

## Part A. Time Analysis (25%)
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)

## Part B. Top Ten Most Popular Services (25%)
Evaluate the top 10 smart contracts by total Ether received. You will need to join address field in the contracts dataset to the to_address in the transactions dataset to determine how much ether a contract has received.

## Part C. Top Ten Most Active Miners (10%)
Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.

## Part D. Data exploration (40%)
The final part of the coursework requires you to explore the data and perform some analysis of your choosing. Below are some suggested ideas for analysis which could be undertaken, along with an expected grade for completing it to a good standard. You may attempt several of these tasks or undertake your own. However, it is recommended to discuss ideas with Joseph before commencing with them.

### Popular Scams
Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time, and does this correlate with certain known scams going offline/inactive? To obtain the marks for this catergory you should provide the id of the most lucrative scam and a graph showing how the ether received has changed over time for the dataset. (20/40)

### Wash Trading
Wash trading is defined as "Entering into, or purporting to enter into, transactions to give the appearance that purchases and sales have been made, without incurring market risk or changing the trader’s market position" Unregulated exchanges use these to fake up to 70% of their trading volume? Which addresses are involved in wash trading? Which trader has the highest volume of wash trades? How certain are you of your result? More information can be found at https://dl.acm.org/doi/pdf/10.1145/3442381.3449824. One way to attempt this is by using Directed Acyclic Graphs (DAGs). Keep in mind if that if you try to load the entire dataset as a graph you may run into memory problems on the cluster so you will need to filter the dataset somewhat before attempting this. This is part of the challenge. Other approaches such as measuring ether balance over time are also possible but you will need to discuss accuracy concerns. Marks will be awarded in a scale relative to the sophistication of your solution. If you can detect simple wash trading between two participants that is worth 20 marks. If you can detect more complicated wash trading between three or four participants that is worth 30 marks. If you can detect wash trading between an arbitrary number of participants that is worth full marks. (40/40)

### Gas Guzzlers
For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? How does this correlate with your results seen within Part B. To obtain these marks you should provide a graph showing how gas price has changed over time, a graph showing how gas_used for contract transactions has changed over time and identify if the most popular contracts use more or less than the average gas_used.(15/40)
