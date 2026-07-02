const fs = require("fs");
const path = require("path");
const {PDFParse}= require("pdf-parse");
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
    path: "http://localhost:8000"
});

const hfClient = new InferenceClient(process.env.GROQ_API_KEY1,{
    endpointUrl: "https://api.groq.com/openai/v1"
});

//const rl = readLine.createInterface(process.stdin, process.stdout);


/*const resumeBuffer = new Uint8Array(fs.readFileSync(path.join(__dirname,"assets","resources","resume.pdf")));
(async ()=>{
    try{

        const parser = new PDFParse(resumeBuffer);
        let data = await parser.getText();
        console.log(data.text);
        let document = data.text;

        const docs = await splitter.createDocuments([document]);
        const textChunkArray = docs.map(doc => doc.pageContent);

        const embeddings = await ollama.embed({
            model: "embeddinggemma",
            input: textChunkArray
        });

        let collection = await client.getOrCreateCollection({name: "resume"});
        await collection.add({
            documents: textChunkArray,
            embeddings: embeddings.embeddings,
            ids: textChunkArray.map((val,idx)=>`resume-chunk-${idx}`)
        });

        console.log("Resume chunks added to ChromaDB collection 'resume' successfully!");

    }catch(err){
        console.log(err);
        throw err;
    }
})();*/
/*async function askQuestion() {
    rl.question("What do you want to know: ", async (query) => {
        try {
            if (query.trim().toLowerCase() === "exit") {
                rl.close();
                return; // Stops the recursion completely
            }

            // 1. Get Embeddings
            let queryEmbedding = await ollama.embed({
                model: "embeddinggemma",
                input: query
            });

            // 2. Query ChromaDB Vector Store
            let collection = await client.getOrCreateCollection({ name: "resume" });
            let results = await collection.query({
                queryEmbeddings: queryEmbedding.embeddings,
                nResults: 5
            });

            console.log(results);

            // 3. Request LLM Stream (Using a proper Groq model ID)
            let stream = await hfClient.chatCompletionStream({
                model: "qwen/qwen3.6-27b", // Use a valid Groq model identifier here
                messages: [
                    {
                        role: "system",
                        content: "You are answering questions on behalf of Ashish, a recent college graduate with a degree in Information Technology. You have access to his resume and will answer questions based on the information in the resume in a conversational way like you yourself are Ashish. You are allowed to be a little creative.Use the resume as the primary source of truth. Reason your answers based on the resume and the relevant chunks provided. If the answer is not in the resume, say 'I don't know'."


                    },
                    {
                        role: "user",
                        content: `Answer the following question based on the resume : ${query}. Here are the relevant context chunks from the resume:\n${results.documents[0].join("\n")}` 
                        // Note: ChromaDB results.documents is usually a nested array [[doc1, doc2, doc3]], so results.documents[0].join is safer.
                    }
                ],
                temperature:0.7
            });

            // 4. Handle Text Streaming Output
            for await (let chunk of stream) {
                if (chunk.choices && chunk.choices[0].delta && chunk.choices[0].delta.content) {
                    process.stdout.write(chunk.choices[0].delta.content);
                }
            }
            console.log("\n");

        } catch (err) {
            console.error("An error occurred:", err);
        }

        // Loop dynamically by calling itself AFTER the asynchronous work finishes
        askQuestion();
    });
}

// Kick off the interactive prompt
askQuestion();*/

async function answerQuery(call){
    const query = call.request.query;
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
        let collection = await client.getOrCreateCollection({ name: "resume" });
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
                    content: "You are answering questions on behalf of Ashish, a recent college graduate with a degree in Information Technology. You have access to his resume and will answer questions based on the information in the resume in a conversational way like you yourself are Ashish. You are allowed to be a little creative.Use the resume as the primary source of truth. Reason your answers based on the resume and the relevant chunks provided. If the answer is not in the resume, say 'I don't know'."
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

const PORT = process.env.PORT || 50051;
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
        console.error("Failed to bind server:", err);
        return;
    }
    console.log(`Server is running on port ${port}`);
});
