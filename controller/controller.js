'use strict'



const {performance} = require('perf_hooks');
const fs = require('fs');
const zlib = require('zlib');
const lzma = require('lzma-native');
const mongoose = require('mongoose');
const moment = require('moment');
const cassandra = require('cassandra-driver');

var Factura = require('../model/factura');
var AgrupacionFactura = require('../model/facturaAgrupada');
var DATA = require('../functions/getData');

const mongoUrl = "mongodb://localhost:27017";
const dbName = 'ticketbai';


function compress_lzma_file(file){
    return new Promise((resolve) => {
        lzma.compress(file, function(result) {
            resolve(result);
        });
    });
}

function insert_mongo(data) {
    return new Promise(resolve => {
        const fact = new Factura();
        fact.collection.insertOne(data, { ordered: false }, (err, docs) => {
            if (err) { console.log(err) }
            else { resolve("Insertados " + docs.length + " datos"); }
        });
    });
}


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
    insercionFacturas: async function(req, res){

        await mongoose.connect(mongoUrl+"/"+dbName).then(() => {console.log("Conexión a MongoDB realizada correctamente")});
        const client = new cassandra.Client({
            contactPoints: ['127.0.0.1'],
            keyspace: 'ticketbai',
            localDataCenter: 'datacenter1'
        });

        client.connect()
            .then(function () {
                console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
                //console.log('Keyspaces: %j', Object.keys(client.metadata.keyspaces));
            })
            .catch(function (err) {
                console.error('There was an error when connecting', err);
                return client.shutdown().then(() => { throw err; });
        });

        var gzip_time_list = [];
        var brotli_time_list = [];
        var lzma_time_list = []

        var gzip_ratio_list = [];
        var brotli_ratio_list = [];
        var lzma_ratio_list = [];

        var mongo_insert_list = [];
        var cassandra_insert_list = [];

        var idents_list = [];

        var labels = [];
        for(var i = 1; i <= 100; i++){
            let factura = fs.readFileSync('./facturas/factura_'+i+'.xml').toString();
            let bytes_start = new TextEncoder().encode(factura).byteLength;

            //GZIP
            var gzip_compresion_start = performance.now();
            let compress_gzip = await zlib.gzipSync(factura, {level: 1});
            var gzip_compresion_fin = performance.now();

            //BROTLI
            var brotli_compresion_start = performance.now();
            let compress_broli = await zlib.brotliCompressSync(factura);
            var brotli_compresion_fin = performance.now();

            //LZMA
            var lzma_compresion_start = performance.now();
            let compress_lzma = await compress_lzma_file(factura)
            var lzma_compresion_fin = performance.now();



            //INSERCION en MONGODB
            var insercion_mongo_start = performance.now();
            let json = {};
            json._id = DATA.getIdentTBAI(factura);
            json.NIF = DATA.getNif(factura);
            json.FechaExpedicionFactura = moment(DATA.getFechaExp(factura), "DD-MM-YYYY").toDate();
            json.HoraExpedicionFactura = moment(DATA.getHoraExpedionFactura(factura), "hh:mm:ss").toDate();
            json.ImporteTotalFactura = DATA.getImporteTotalFactura(factura);
            json.SerieFactura = DATA.getSerieFactura(factura);
            json.NumFactura = DATA.getNumFactura(factura);
            json.Descripcion = DATA.getDescripcion(factura);
            json.FacturaComprimida = compress_gzip.toString("base64");
            json.Status = 0;
            await insert_mongo(json);
            var insercion_mongo_fin = performance.now();

            //Insercion en Cassandra
            var insercion_cassandra_start = performance.now();
            const insertQuery = "insert into facturas (nif, fecha, tbai_id, importe, num_factura, serie, xml) values (?, ?, ?, ?, ?, ?, ?)";
            const params = [
                DATA.getNif(factura),
                moment(DATA.getFechaExp(factura), "DD-MM-YYYY").toDate(),
                DATA.getIdentTBAI(factura),
                DATA.getImporteTotalFactura(factura),
                DATA.getNumFactura(factura),
                DATA.getSerieFactura(factura),
                compress_gzip.toString('base64')
            ];
            await client.execute(insertQuery, params, { prepare: true });
            var insercion_cassandra_fin = performance.now();


            gzip_time_list.push(gzip_compresion_fin-gzip_compresion_start);
            brotli_time_list.push(brotli_compresion_fin-brotli_compresion_start);
            lzma_time_list.push(lzma_compresion_fin-lzma_compresion_start);

            gzip_ratio_list.push(1-(compress_gzip.byteLength / bytes_start));
            brotli_ratio_list.push(1-(compress_broli.byteLength / bytes_start));
            lzma_ratio_list.push(1-(Buffer.byteLength(compress_lzma) / bytes_start));
            
            mongo_insert_list.push(insercion_mongo_fin-insercion_mongo_start);
            cassandra_insert_list.push(insercion_cassandra_fin-insercion_cassandra_start);

            labels.push(i);

            //fs.writeFileSync('./files/tbai_saved_idents.txt', json._id+"\n", {flag:'a'});
            idents_list.push(json._id);
            //idents_list.push(DATA.getIdentTBAI(factura));
        }




        let script_time = ' <script> '+
            'var ctx = document.getElementById("compress_time_chart");'+
            'const labels_time = ["'+labels.join('\","')+'"];'+
            'const data_time = {'+
              'labels: labels_time,'+
              'datasets : [{'+
                'label: "GZip",'+
                'data: ['+ gzip_time_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(75, 192, 192)",'+
                'tension: 0.1},'+
                '{label: "Brotli",'+
                'data: ['+ brotli_time_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(255, 0, 0)",'+
                'tension: 0.1},'+
                '{label: "LZMA",'+
                'data: ['+ lzma_time_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(0, 255, 0)",'+
                'tension: 0.1}'+
              ']'+
            '};'+
      
            'const config_time = {'+
              'type: "line",'+
              'data: data_time'+
            '};'+
            'var chart = new Chart(ctx, config_time);</script>';

            

            let script_ratio = ' <script> '+
            'var ctx = document.getElementById("compress_ratio_chart");'+
            'const labels_ratio = ["'+labels.join('\","')+'"];'+
            'const data_ratio = {'+
              'labels: labels_ratio,'+
              'datasets : [{'+
                'label: "GZip",'+
                'data: ['+ gzip_ratio_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(75, 192, 192)",'+
                'tension: 0.1},'+
                '{label: "Brotli",'+
                'data: ['+ brotli_ratio_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(255, 0, 0)",'+
                'tension: 0.1},'+
                '{label: "LZMA",'+
                'data: ['+ lzma_ratio_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(0, 255, 0)",'+
                'tension: 0.1},'+
              ']'+
            '};'+
      
            'const config_ratio = {'+
              'type: "line",'+
              'data: data_ratio'+
            '};'+
            'var chart = new Chart(ctx, config_ratio);</script>';


            let script_insert = ' <script> '+
            'var ctx = document.getElementById("insert_chart");'+
            'const labels_insert = ["'+labels.join('\","')+'"];'+
            'const data_insert = {'+
              'labels: labels_insert,'+
              'datasets : [{'+
                'label: "Mongo",'+
                'data: ['+ mongo_insert_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(75, 192, 192)",'+
                'tension: 0.1},'+
                '{label: "Cassandra",'+
                'data: ['+ cassandra_insert_list.toString()+'],'+
                'fill:false,'+
                'borderColor: "rgb(255, 0, 0)",'+
                'tension: 0.1}'+
              ']'+
            '};'+
      
            'const config_insert = {'+
              'type: "line",'+
              'data: data_insert'+
            '};'+
            'var chart = new Chart(ctx, config_insert);</script>';

            const query = {
                _id: {
                    $in: idents_list
                }
            };
            Factura.deleteMany(query, function(err, result){
                if(!err){
                    console.log("Datos eliminados correctamente");
                }
            });

            const delete_query = "delete from facturas where nif in ? and fecha in ? and tbai_id in ?";
            var nif_list = idents_list.map(n => n.split("-")[1]);
            var fecha_list = idents_list.map(n => moment(n.split("-")[2], "DDMMYY").format("YYYY-MM-DD"));
            var delete_params = [
                nif_list,
                fecha_list,
                idents_list,
            ];
            client.execute(delete_query, delete_params, {prepare: true}).then(() => console.log("Datos de cassandra eliminados"));


        res.status(200).render('insercion', {
            title: 'Inserción de facturas',
            page: 'insercion',
            script_time: script_time,
            script_ratio: script_ratio,
            script_insert: script_insert
        });

    },
    fileCompress: async function(req, res) {//Compresión y descompresión del fichero
        //Tengo que comprimir el fichero con las distintas tecnicas de compresión,
        //descomprimirlo y devolver un json con los resultados.

        var fileName = 'Archivo no subido...';

        if(req.files){
            var filePath = req.files.file.path;
            var fileSize = req.files.file.size;
            var fileName = filePath.split('\\')[1];




            var graphic_labels = ["GZip", "Brotli", "Lzma", "Lzma2", "Lzss"];
            var graphic_data = [];

            const file = fs.readFileSync(filePath).toString();
            let bytes_init = new TextEncoder().encode(file).byteLength;
            var start_gzip = performance.now();
            let gzip_compress = zlib.gzipSync(file, {level: 1}).toString("base64");
            var fin_gzip = performance.now();
            graphic_data.push(fin_gzip-start_gzip);


            /*
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
*/
            

            let script = ' <script> '+
            'var ctx = document.getElementById("myChart");'+
            'const labels = ["'+graphic_labels.join('\","')+'"];'+
            'const data = {'+
              'labels: labels,'+
              'datasets : [{'+
                'label: "Label",'+
                'data: [193.3979,158.1588,159.9402,162.4047,164.1935],'+
                'fill:false,'+
                'borderColor: "rgb(75, 192, 192)",'+
                'tension: 0.1'+
              '}]'+
            '};'+
      
            'const config = {'+
              'type: "line",'+
              'data: data'+
            '};'+
            'var chart = new Chart(ctx, config);</script>';

            return res.status(200).render(
                'result',
                {
                    title: 'Resultados de la Compresión',
                    page: 'resultCompresion',
                    script: script
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