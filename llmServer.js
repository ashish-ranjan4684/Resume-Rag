const fs = require("fs");
const path = require("path");
const {RecursiveCharacterTextSplitter} = require("@langchain/textsplitters");
const {Ollama} = require("ollama");
const {ChromaClient} = require("chromadb");
const {InferenceClient} = require("@huggingface/inference");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
require("dotenv").config();

const packageDef = protoLoader.loadSync(path.join(__dirname, "aichat.proto"));
const proto = grpc.loadPackageDefinition(packageDef).aichat;

const splitter = new RecursiveCharacterTextSplitter({
    chunkSize: 200,
    chunkOverlap:40
});
const ollama = new Ollama({
    host: "http://localhost:11434",
});

const client = new ChromaClient({
    host:"localhost",
    port:8000
});

const hfClient = new InferenceClient(process.env.GROQ_API_KEY1,{
    endpointUrl: "https://api.groq.com/openai/v1"
});

async function answerQuery(call){
    const query = call.request.query;
    const clientUser = call.request.client;
    console.log(`Received query: ${query}`);
    if(!query){
        return call.destroy({
            code: grpc.status.INVALID_ARGUMENT,
            message: "Query parameter is required"
        });
    }

    call.on("error", (err)=>{
        console.error("Stream error:", err);
        return call.destroy({
            code: grpc.status.INTERNAL,
            message: "Internal server error"
        });
    });

    /*res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");*/

    try {
        // 1. Get Embeddings
        let queryEmbedding = await ollama.embed({
            model: "embeddinggemma",
            input: query
        });

        // 2. Query ChromaDB Vector Store
        let collection = await client.getOrCreateCollection({ name: `${clientUser}-resume` });
        let results = await collection.query({
            queryEmbeddings: queryEmbedding.embeddings,
            nResults: 5
        });

        //3. Request LLM Stream (Using a proper Groq model ID)
        let stream = await hfClient.chatCompletionStream({
            model: "qwen/qwen3.6-27b", // Use a valid Groq model identifier here
            messages: [
                {
                    role: "system",
                    content: `You are answering questions on behalf of ${clientUser}, a recent college graduate with a degree in Information Technology. You have access to his resume and will answer questions based on the information in the resume in a conversational way like you yourself are Ashish. You are allowed to be a little creative.Use the resume as the primary source of truth. Reason your answers based on the resume and the relevant chunks provided. If the answer is not in the resume, say 'I don't know'.`
                },
                {
                    role: "user",
                    content: `Answer the following question based on the resume : ${query}. Here are the relevant context chunks from the resume:\n${results.documents[0].join("\n")}` 
                }
            ],
            temperature:0.7,

            reasoning_format: "hidden"
        });

        // 4. Handle Text Streaming Output
        for await (let chunk of stream) {
            if (chunk.choices && chunk.choices[0].delta && chunk.choices[0].delta.content) {
                //process.stdout.write(chunk.choices[0].delta.content);
                //res.write(chunk.choices[0].delta.content);
                call.write({ answer: chunk.choices[0].delta.content });
                console.log("sent chunk");
            }
            
        }
        call.end();
    } catch (err) {
        console.error("An error occurred:", err);
        //res.write("\n[System Error: Unable to complete stream configuration]");
        //res.end();
        return call.destroy({
            code: grpc.status.INTERNAL,
            message: "Internal server error"
        })
    }
};

const server = new grpc.Server();
server.addService(proto.AiChatService.service, {
    AnswerQuery: answerQuery
});

const PORT = process.env.LLM_PORT1 || 50051;
server.bindAsync(`10.0.0.6:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
        console.error("Failed to bind server:", err);
        return;
    }
    console.log(`Server is running on port ${port}`);
});
