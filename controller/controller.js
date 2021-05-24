'use strict'



const { performance } = require('perf_hooks');
const fs = require('fs');
const zlib = require('zlib');
const lzma = require('lzma-native');
const lzss = require('lzbase62');
const mongoose = require('mongoose');
const moment = require('moment');
const cassandra = require('cassandra-driver');
const qrcode = require('qrcode');

var Factura = require('../model/factura');
var AgrupacionFactura = require('../model/facturaAgrupada');
var DATA = require('../functions/getData');

const mongoUrl = "mongodb://localhost:27017";
const dbName = 'ticketbai';


function compress_lzma_file(file) {
    return new Promise((resolve) => {
        lzma.compress(file, function (result) {
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

function compressData(data) {
    return new Promise((resolve) => {
        zlib.gzip(data, { level: 1 }, (err, result) => {
            if (!err) resolve(result.toString('base64'),
            );
        });
    });
}

function unCompressData(data) {
    return new Promise((resolve, reject) => {
        zlib.gunzip(Buffer.from(data, "base64"), (err, result) => {
            if (!err) resolve(result.toString());
            reject(err);
        });
    });
}


var controller = {
    index: function (req, res) {
        return res.status(200).render('index', { title: 'Express', page: 'index' });
    },
    insertFactura: function (req, res) {
        return res.status(200).render(
            'insert',
            {
                title: 'Inserción de Facturas',
                page: 'insert'
            }
        );
    },
    tctest: async function (req, res) {
        var num = req.query.num;
        if (num == null || num == 0 || num > 5000) {
            num = 100;
        }
        /*
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
        */
        var gzip_time_list = [];
        var brotli_time_list = [];
        var lzma_time_list = [];
        var lzss_time_list = [];

        var gzip_ratio_list = [];
        var brotli_ratio_list = [];
        var lzma_ratio_list = [];
        var lzss_ratio_list = [];

        var mongo_insert_list = [];
        var cassandra_insert_list = [];

        var idents_list = [];

        var labels = [];
        for (var i = 1; i <= num; i++) {
            let factura = fs.readFileSync('./facturas/factura_' + i + '.xml').toString();
            let bytes_start = new TextEncoder().encode(factura).byteLength;

            //GZIP
            var gzip_compresion_start = performance.now();
            let compress_gzip = await zlib.gzipSync(factura, { level: 1 });
            var gzip_compresion_fin = performance.now();

            //BROTLI
            var brotli_compresion_start = performance.now();
            let compress_broli = await zlib.brotliCompressSync(factura);
            var brotli_compresion_fin = performance.now();

            //LZMA
            var lzma_compresion_start = performance.now();
            let compress_lzma = await compress_lzma_file(factura);
            var lzma_compresion_fin = performance.now();

            //LZSS
            var lzss_compresion_start = performance.now();
            var compress_lzss = await lzss.compress(factura);
            var lzss_compresion_fin = performance.now();

            //INSERCION en MONGODB
            /*      var insercion_mongo_start = performance.now();
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
      */

            gzip_time_list.push(gzip_compresion_fin - gzip_compresion_start);
            brotli_time_list.push(brotli_compresion_fin - brotli_compresion_start);
            lzma_time_list.push(lzma_compresion_fin - lzma_compresion_start);
            lzss_time_list.push(lzss_compresion_fin - lzss_compresion_start);

            gzip_ratio_list.push(1 - (compress_gzip.byteLength / bytes_start));
            brotli_ratio_list.push(1 - (compress_broli.byteLength / bytes_start));
            lzma_ratio_list.push(1 - (Buffer.byteLength(compress_lzma) / bytes_start));
            lzss_ratio_list.push(1 - (Buffer.byteLength(compress_lzss) / bytes_start));

            // mongo_insert_list.push(insercion_mongo_fin-insercion_mongo_start);
            // cassandra_insert_list.push(insercion_cassandra_fin-insercion_cassandra_start);

            labels.push(i);

            //fs.writeFileSync('./files/tbai_saved_idents.txt', json._id+"\n", {flag:'a'});
            //   idents_list.push(json._id);
            //idents_list.push(DATA.getIdentTBAI(factura));
        }




        let script_time = ' <script> ' +
            'var ctx = document.getElementById("compress_time_chart");' +
            'const labels_time = ["' + labels.join('\","') + '"];' +
            'const data_time = {' +
            'labels: labels_time,' +
            'datasets : [{' +
            'label: "GZip",' +
            'data: [' + gzip_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(75, 192, 192)",' +
            'tension: 0.1},' +
            '{label: "Brotli",' +
            'data: [' + brotli_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(255, 0, 0)",' +
            'tension: 0.1},' +
            '{label: "LZMA",' +
            'data: [' + lzma_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(0, 255, 0)",' +
            'tension: 0.1},' +
            '{label: "LZSS",' +
            'data: [' + lzss_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(255, 255, 0)",' +
            'tension: 0.1}' +
            ']' +
            '};' +

            'const config_time = {' +
            'type: "line",' +
            'data: data_time' +
            '};' +
            'var chart = new Chart(ctx, config_time);</script>';



        let script_ratio = ' <script> ' +
            'var ctx = document.getElementById("compress_ratio_chart");' +
            'const labels_ratio = ["' + labels.join('\","') + '"];' +
            'const data_ratio = {' +
            'labels: labels_ratio,' +
            'datasets : [{' +
            'label: "GZip",' +
            'data: [' + gzip_ratio_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(75, 192, 192)",' +
            'tension: 0.1},' +
            '{label: "Brotli",' +
            'data: [' + brotli_ratio_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(255, 0, 0)",' +
            'tension: 0.1},' +
            '{label: "LZMA",' +
            'data: [' + lzma_ratio_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(0, 255, 0)",' +
            'tension: 0.1},' +
            '{label: "LZSS",' +
            'data: [' + lzss_ratio_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(255, 255, 0)",' +
            'tension: 0.1},' +
            ']' +
            '};' +

            'const config_ratio = {' +
            'type: "line",' +
            'data: data_ratio' +
            '};' +
            'var chart = new Chart(ctx, config_ratio);</script>';


        /*       let script_insert = ' <script> '+
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
   */
        /*         const query = {
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
     */

        res.status(200).render('tctest', {
            title: 'Inserción de Facturas',
            page: 'tctest',
            script_time: script_time,
            script_ratio: script_ratio//,
            // script_insert: script_insert
        });

    },
    getFactura: async function (req, res) {

        console.log("getFactura");
        res.status(200).render('getfactura', {
            title: 'Búsqueda de Facturas',
            page: 'getFactura'
        });
    },
    gr: async function (req, res) {
        const regex = /^TBAI-[0-9]{8}[A-Z]-[0-9]{6}-.{13}-[0-9]{3}$/;;
        //document.getElementById('loading_gif').style.display = "block";
        var tbai_id = String(req.query.id);
        if (!tbai_id.match(regex)) {
            res.status(200).render('gr', {
                title: 'Búsqueda de Facturas',
                page: 'getFactura',
                error: '<script>document.getElementById("data").style.display = "none";</script><script>document.getElementById("tbai_error").style.display = "block";</script>',
                data: ""
            });
        } else {

            await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
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
            

            var result_mongo = await findByTBAI(tbai_id);

            var result_cassandra = await findByIdCassandra(tbai_id, client);



            let script = ' <script> ' +
            'var ctx = document.getElementById("get_factura_chart");' +
            'const labels = ["MongoDB", "Cassandra"];' +
            'const data = {' +
            'labels: labels,'+
            'datasets:[{'+
            'label: "Tiempo de Obtención de Datos (milisegundos)",' +
            'data: ['+(result_mongo.stats.busqueda_datos)+','+(result_cassandra.stats.busqueda_datos)+'],' +
            'backgroundColor: ["rgba(255, 99, 132, 0.2)", "rgba(54, 162, 235, 0.2)"],' +
            'borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],'+
            'borderWidth: 1'+
            '}]' +
            '};'+

            'const config = {' +
            'type: "bar",' +
            'data: data' +
            '};' +
            'var chart = new Chart(ctx, config);</script>';


            res.status(200).render('gr', {
                title: 'Búsqueda de Facturas',
                page: 'getFactura',
                error: '<script>document.getElementById("data").style.display = "block";</script><script>document.getElementById("tbai_error").style.display = "none";</script>',
                tbai_id: result_mongo.data.tbai_id,
                nif_emisor: result_mongo.data.nif_emisor,
                serie_factura: result_mongo.data.serie_factura,
                num_factura: result_mongo.data.num_factura,
                importe_factura: result_mongo.data.importe_factura + " €",
                fecha_exp: moment(result_mongo.data.fecha_exp).format("YYYY/MM/DD"),
                script: script
            });
        }



    },
    insercionFacturas: async function (req, res) {//Compresión y descompresión del fichero
        //Tengo que comprimir el fichero con las distintas tecnicas de compresión,
        //descomprimirlo y devolver un json con los resultados.

        var fileName = 'Archivo no subido...';

        if (req.files) {
            var filePath = req.files.file.path;
            var fileExt = filePath.split('\\')[1].split(".")[1];



            if (fileExt == "xml") {//Simplemente lo subo
                await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
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
                var factura = fs.readFileSync(filePath).toString();
                //INSERCION en MONGODB
                var compress_gzip = await compressData(factura);
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
                //json.FacturaComprimida = compress_gzip.toString("base64");
                json.FacturaComprimida = compress_gzip;
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
                    compress_gzip
                ];
                await client.execute(insertQuery, params, { prepare: true });
                var insercion_cassandra_fin = performance.now();

                res.status(200).send({
                    tbai_id: json._id
                });


            } else if (fileExt == "zip") {//Descomprimo las facturas y las agrupo
                res.status(200).send(
                    {
                        title: 'Inserción de Facturas',
                        page: 'insertFacturas',
                        tbai_id: "TBAI-SADSADSA"
                        //file: compressed
                    }
                );
            } else {//Error el formato no es correcto

            }

            /* res.status(200).render(
                 'result',
                 {
                     title: 'Inserción de Facturas',
                     page: 'insertFacturas'
                 }
             );*/
        } else {
            res.status(200).send(
                {
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas',
                    files: "Error al enviar los archivos"
                    //file: compressed
                }
            );
        }
    }

};





async function findByIdCassandra(tbai_id, client) {
    return new Promise((resolve) => {
        const query_indiv = "select nif, fecha, tbai_id, importe, num_factura, serie from facturas where nif=? and fecha=? and tbai_id = ?";
        const params_indiv = [
            tbai_id.split("-")[1],
            moment(tbai_id.split("-")[2], "DDMMYY").format("YYYY-MM-DD"),
            tbai_id
        ];

        console.log("dfsadasd");
        var busqueda_bd_start = performance.now();
        client.execute(query_indiv, params_indiv, { prepare: true }).then((resul) => {
            if (resul.rowLength < 1) {

            } else {
                var busqueda_bd_fin = performance.now();

                resolve({
                    data: {
                        tbai_id: tbai_id,
                        nif_emisor: resul.rows[0].nif,
                        serie_factura: resul.rows[0].serie,
                        num_factura: resul.rows[0].num_factura,
                        importe_factura: resul.rows[0].importe,
                        fecha_exp: resul.rows[0].fecha
                    },
                    stats: {
                        busqueda_datos: busqueda_bd_fin - busqueda_bd_start
                    }
                });
            }
        });

    });
}


/**
 * Busca la factura que coincide con el id que se le pasa como parametro. Primero busca en la coleccion Facturas,
 * que es la coleccion que contiene las facturas unitarias. Si no lo encuentra, busca en la coleccion de facturas_agrupadas,
 * y devulve la factura correspondiente.
 * @param {string} tbai_id identificador de la factura
 * @returns La factura cuyo identificador coincide con tbai_id
 */
async function findByTBAI(tbai_id) {
    return new Promise((resolve) => {
        var busqueda_datos_start = performance.now();
        Factura.findById(tbai_id, '_id NIF FechaExpedicionFactura ImporteTotalFactura SerieFactura NumFactura', (err, factura) => {
            var busqueda_datos_fin = performance.now();
            if (err) resolve({
                code: 500,
                data: "Error al devolver los datos",
                stats: {}
            });
            if (!factura) {//No he encontrado una factura con esa id (Estará comprimida)
                //TBAI-82275936Z-010120-dPmfSHpsGqWpI-120
                var tbai_split = tbai_id.split("-");
                let nif = tbai_split[1];
                let fecha = moment(tbai_split[2], "DDMMYY").toDate();
                busqueda_datos_start = performance.now();
                AgrupacionFactura.find({ nif: nif, fechaInicio: { $lte: fecha }, fechaFin: { $gte: fecha } }, (err, docs) => {
                    var busqueda_datos_fin = performance.now();
                    if (err) resolve({
                        code: 500,
                        data: "Error al devolver los datos",
                        stats: {}
                    });
                    if (!docs) resolve({
                        code: 404,
                        data: "No se ha encontrado la factura con ese identificador",
                        stats: {}
                    });

                    for (var i = 0; i < docs.length; i++) {
                        if (docs[i].idents.includes(tbai_id)) {
                            var pos = Array.from(docs[i].idents).indexOf(tbai_id);
                            var descompresion_start = performance.now();

                            unCompressData(docs[i].agrupacion).then((resul) => {
                                var descompresion_fin = performance.now();

                                var busqueda_factura_start = performance.now();
                                var facturas_array = resul.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                                let data = facturas_array[pos];
                                var busqueda_factura_fin = performance.now();
                                resolve({
                                    code: 200,
                                    data: data,
                                    stats: {
                                        busqueda_datos: busqueda_datos_fin - busqueda_datos_start,
                                        descompresion: descompresion_fin - descompresion_start,
                                        busqueda_factura: busqueda_factura_fin - busqueda_factura_start
                                    }
                                });
                            });

                        }
                    }
                });
            } else {
                resolve({
                    code: 200,
                    data: {
                        tbai_id: tbai_id,
                        nif_emisor: factura.NIF,
                        serie_factura: factura.SerieFactura,
                        num_factura: factura.NumFactura,
                        importe_factura: factura.ImporteTotalFactura,
                        fecha_exp: factura.FechaExpedicionFactura
                    },
                    stats: {
                        busqueda_datos: busqueda_datos_fin - busqueda_datos_start
                    }
                });


                /*var descompresion_start = performance.now();
                unCompressData(factura.FacturaComprimida).then((resul) => {
                    var descompresion_fin = performance.now();
                    //fs.writeFileSync('./files/factura_pequena.xml', resul);
                    resolve({
                        code: 200,
                        xml: resul,
                        data: {
                            tbai_id: tbai_id,
                            nif_emisor: factura.NIF,
                            serie_factura: factura.SerieFactura,
                            num_factura: factura.NumFactura,
                            importe_factura: factura.ImporteTotalFactura,
                            fecha_exp: factura.FechaExpedicionFactura
                        },
                        stats: {
                            busqueda_datos: busqueda_datos_fin - busqueda_datos_start,
                            descompresion: descompresion_fin - descompresion_start
                        }
                    });
                });*/
            }
        });
    });

}


module.exports = controller;