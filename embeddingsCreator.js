const path = require("path");
const {PDFParse}= require("pdf-parse");
const {RecursiveCharacterTextSplitter} = require("@langchain/textsplitters");
const {Ollama} = require("ollama");
const {ChromaClient} = require("chromadb");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const crypto = require("crypto");
const fs = require("fs");
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
    //path: "http://localhost:8000"
    host:"localhost",
    port:8000,
});

if(!fs.existsSync(path.join(__dirname,"assets","resources"))){
    fs.mkdirSync(path.join(__dirname,"assets","resources"),{
        recursive:true
    });
}

async function createResumeChunksAndEmbeddings(resumePdf, user){
    try{
        const resumeBuffer = new Uint8Array(fs.readFileSync(path.join(__dirname,"assets","resources",resumePdf)));
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
        let collectionName = `${user}-resume`
        let collection = await client.getOrCreateCollection({name: collectionName});
        await collection.upsert({
            documents: textChunkArray,
            embeddings: embeddings.embeddings,
            ids: textChunkArray.map((val,idx)=>`resume-chunk-${idx}`)
        });

        console.log(`Resume chunks added to ChromaDB collection ${collectionName}  successfully!`);

    }catch(err){
        console.log(err);
        throw err;
    }
}

function createResumeEmbeddings(call,callback){

    let resumeText = "";
    let uploadedFileSize = 0;
    let totalSize = 0;
    let ws;
    let user;
    let error = {};
    let sha256Hash = crypto.createHash("sha256");
    call.on("data", (msg) =>{
        if(msg.user){
            user = msg.user;
            console.log(`Request for resume chunking and embedding creation received for user ${user}`);
            return;
        }
        if(msg.metadata){
            totalSize = msg.metadata.totalSize;
            console.log(`Total size of the resume file to be received: ${totalSize} bytes`);
            ws = fs.createWriteStream(path.join(__dirname,"assets","resources",`${user}-resume.pdf`));
            ws.on("error", (err)=>{
                console.error("Error writing to file:", err);
                error.code = grpc.status.INTERNAL;
                error.message = "Error writing to file";
                call.write(error);
                call.end();
            });
            call.write({
                code: grpc.status.OK,
                message: "Ready to receive resume chunks"
            });
        }
        if(msg.hash){
            let checksum = sha256Hash.digest("hex");
            if(checksum !== msg.hash){
                console.error("Hash mismatch! Data integrity compromised.");
                error.code = grpc.status.DATA_LOSS;
                error.message = "Hash mismatch! Data integrity compromised.";    
                call.write(error);

                if(ws && !ws.writableEnded){
                    ws.end();
                }
                call.end();
            }else{
                console.log("Hash verified successfully. Data integrity intact.");
                ws.end();

                ws.on("finish",async()=>{
                    try{
                        await createResumeChunksAndEmbeddings(`${user}-resume.pdf`, user);
                        call.write({code:grpc.status.OK,
                            message: "Resume chunks and embeddings created successfully!"
                        });
                        call.end();
                    }catch(err){
                        console.error("Error occurred while creating resume chunks and embeddings:", err);
                        call.write({code:grpc.status.INTERNAL,
                            message: "An error occurred while processing the resume."});
                        call.end();
                    }
                })
                
            }
            return;
        
        }
        if(msg.chunk){
            if(!ws || !user){
                console.error("Received chunk before metadata or user information. Aborting.");
                error.code = grpc.status.FAILED_PRECONDITION;
                error.message = "Received chunk before metadata or user information.";
                call.write(error);
                call.end();
                return;
            }
            ws.write(msg.chunk);
            uploadedFileSize += msg.chunk.length;
            let percentComplete = ((uploadedFileSize / totalSize) * 100).toFixed(2);
            console.log(`Received ${uploadedFileSize} bytes of ${totalSize} bytes (${percentComplete}%)`);
            sha256Hash.update(msg.chunk);
            call.write({
                code: grpc.status.OK,
                message:`${percentComplete}%`
            });
        }
    });

    call.on("end", ()=>{
        if(ws && !ws.writableEnded){
            ws.end();
        }
    });
}

const server = new grpc.Server();
server.addService(proto.AiChatService.service, {
    CreateResumeEmbeddings: createResumeEmbeddings
});

const PORT = process.env.EMBEDDING_PORT || 50053;
server.bindAsync(`10.0.0.6:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
        console.error("Failed to bind server:", err);
        return;
    }
    console.log(`Server is running on port ${port}`);
});