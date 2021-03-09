'use strict'

const {gzip, ungzip} = require('node-gzip');


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
            var fileExt = fileName.split('.')[1];
/*
            gzip(filePath).then((compressed) => {
                return res.status(200).render(
                    'result',
                    {
                        title: 'Resultados de la Compresión',
                        page: 'resultCompresion',
                        file: compressed
                    }
                );
            })*/

            return res.status(200).render(
                'result',
                {
                    title: 'Resultados de la Compresión',
                    page: 'resultCompresion',
                    files: req.files
                    //file: compressed
                }
            );
                
            }
        }

};


module.exports = controller;