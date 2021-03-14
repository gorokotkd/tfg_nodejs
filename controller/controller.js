'use strict'

const {gzip, ungzip} = require('node-gzip');
const brotli = require('brotli');
const {performance} = require('perf_hooks');
const fs = require('fs');
const output = require('d3node-output');
const d3 = require('d3-node')().d3;
const d3nBar = require('d3node-barchart');

/*
let createBarchart = new Promise((resolve, reject) => {
    const csv = fs.readFileSync('./data/data.csv').toString();
    const data = d3.csvParse(csv);
    output('./public/output', d3nBar({data: data}));

    if(fs.existsSync('./public/output.png')){
        resolve('./public/output.png');
    }else{
        reject('Error file doesn\'t exists');
    }

    
});*/



var controller = {
    index: function(req, res){
        return res.status(200).render('index', {title: 'Express', page: 'index'});
    },
    tctest: function(req, res){
        return res.status(200).render(
            'tctest',
            {
                title: 'Técnicas de Compresión',
                page: 'tctest'
            }
        );
    },
    fileCompress: function(req, res) {//Compresión y descompresión del fichero
        //Tengo que comprimir el fichero con las distintas tecnicas de compresión,
        //descomprimirlo y devolver un json con los resultados.

        var fileName = 'Archivo no subido...';
        console.log(req.files);
        if(req.files){
            var filePath = req.files.file.path;
            var fileSize = req.files.file.size;
            var fileName = filePath.split('\\')[1];
            //var fileExt = fileName.split('.')[1];
                                                        //Algorithm, Compression Speed
            fs.writeFileSync('./data/compress-time.csv','key,value\n');
                                                        //Algorithm, Decompression Speed
            fs.writeFileSync('./data/decompress-time.csv','key,value\n');
                                                        //Algorithm, Compression Ratio
            fs.writeFileSync('./data/compress-ratio.csv','key,value\n');
            fs.writeFileSync('./data/compress-ratio.csv', 'OriginalFile,'+fileSize+"\n", {flag: 'a'});


            var start_gzip = performance.now();
            gzip(filePath).then((compressed) => {//GZIP COMPRESSION
                var fin_gzip = performance.now();
                var string_gzip = "Gzip," + (fin_gzip - start_gzip).toString() + "\n";
                fs.writeFileSync('./data/compress-time.csv',string_gzip, {flag: 'a'});
                fs.writeFileSync('./data/compress-ratio.csv','Gzip,'+Buffer.byteLength(compressed)+"\n", {flag: 'a'});

                start_gzip = performance.now();
                ungzip(compressed).then((buf) => {
                    fin_gzip = performance.now();
                    string_gzip = "Gzip," + (fin_gzip - start_gzip).toString() + "\n";
                    fs.writeFileSync('./data/decompress-time.csv',string_gzip, {flag: 'a'});
                });
                
            });
            
            var start_brotli = performance.now();
            var comp_buf = brotli.compress(fs.readFileSync(filePath))
            var fin_brotli = performance.now();
            var string_brotli = "Brotli," + (fin_brotli - start_brotli).toString() + "\n";
            fs.writeFileSync('./data/compress-time.csv',string_brotli, {flag: 'a'});
            fs.writeFileSync('./data/compress-ratio.csv','Brotli,'+Buffer.byteLength(comp_buf)+"\n", {flag: 'a'});

            start_brotli = performance.now();
            brotli.decompress(comp_buf);
            fin_brotli = performance.now();
            string_brotli = "Brotli," + (fin_brotli - start_brotli).toString() + "\n";
            fs.writeFileSync('./data/decompress-time.csv',string_brotli, {flag: 'a'});

            


            var csv = fs.readFileSync('./data/compress-time.csv').toString();
            var data = d3.csvParse(csv);
            output('./public/compress-time', d3nBar({data: data}));

            var csv = fs.readFileSync('./data/decompress-time.csv').toString();
            var data = d3.csvParse(csv);
            output('./public/decompress-time', d3nBar({data: data}));

            var csv = fs.readFileSync('./data/compress-ratio.csv').toString();
            var data = d3.csvParse(csv);
            output('./public/compress-ratio', d3nBar({data: data}));


            fs.unlinkSync(filePath);    
            return res.status(200).render(
                'result',
                {
                    title: 'Resultados de la Compresión',
                    page: 'resultCompresion'
                }
            );
        }else{
            return res.status(200).render(
                'result',
                {
                    title: 'Resultados de la Compresión',
                    page: 'resultCompresion',
                    files: "Error al enviar los archivos"
                    //file: compressed
                }
            );
        }
    }

};





module.exports = controller;