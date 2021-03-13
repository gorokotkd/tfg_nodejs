'use strict'

const {gzip, ungzip} = require('node-gzip');
const {performance} = require('perf_hooks');
const fs = require('fs');
const output = require('d3node-output');
const d3 = require('d3-node')().d3;
const d3nBar = require('d3node-barchart');


let createBarchart = new Promise((resolve, reject) => {
    const csv = fs.readFileSync('./data/data.csv').toString();
    const data = d3.csvParse(csv);
    output('./public/output', d3nBar({data: data}));

    if(fs.existsSync('./public/output.png')){
        resolve('./public/output.png');
    }else{
        reject('Error file doesn\'t exists');
    }

    
});


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
            var fileName = filePath.split('\\')[1];
            //var fileExt = fileName.split('.')[1];
            var start = performance.now();
            
            gzip(filePath).then((compressed) => {
                var fin = performance.now();

                createBarchart.then((mes) => {
                    console.log(mes);
                    return res.status(200).render(
                        'result',
                        {
                            title: 'Resultados de la Compresión',
                            page: 'resultCompresion',
                            gzip: {
                                time: fin - start,
                                fileName: fileName, 
                                compression: compressed
                            },
                        }
                    );
                });
/*
                return res.status(200).render(
                    'result',
                    {
                        title: 'Resultados de la Compresión',
                        page: 'resultCompresion',
                        gzip: {
                            time: fin - start,
                            fileName: fileName, 
                            compression: compressed
                        },
                    }
                );*/

            });  
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