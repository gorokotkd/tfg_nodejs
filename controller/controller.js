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
const parser = require('xml-js');

var Factura = require('../model/factura');
var AgrupacionFactura = require('../model/facturaAgrupada');
var DATA = require('../functions/getData');
const { db } = require('../model/factura');
const createFacturas = require('../functions/createData');
const { companies_nif_list } = require('../functions/companies_nif');

const mongoUrl = "mongodb://localhost:27017";
const dbName = 'ticketbai';

const FACTURAS_AGRUPADAS_PATH = "../facturas/";
const MB = 1000000;

const GZIP_PARAMS = {
    level: 1,
    flush: zlib.constants.BROTLI_OPERATION_PROCESS,
    finishFlush: zlib.constants.BROTLI_OPERATION_FINISH,
    chunkSize: 16 * 1024
};

function compress_lzma_file(file) {
    return new Promise((resolve) => {
        lzma.compress(file, function (result) {
            resolve(result);
        });
    });
}

function decompress_lzma_file(file) {
    return new Promise((resolve) => {
        lzma.decompress(file, function (result) {
            resolve(result);
        });
    });
}

function insert_mongo(data) {
    return new Promise((resolve, reject) => {
        const fact = new Factura();
        fact.collection.insertMany(data, { ordered: false }, (err, docs) => {
            if (err) { reject(err) }
            else { resolve(); }
        });
    });
}

function insert_agrupadas_mongo(data) {
    return new Promise((resolve) => {
        const group = new AgrupacionFactura();
        group.collection.insertMany(data, { ordered: false }, (err, docs) => {
            if (err) { console.log(err); }
            else { resolve("Insertadas " + docs.length + " agrupaciones"); }
        });
    });
}

function compressData(data) {
    return new Promise((resolve) => {
        zlib.gzip(data, GZIP_PARAMS, (err, result) => {
            if (!err) resolve(result.toString('base64'),
            );
        });
    });
}

function unCompressData(data) {
    return new Promise((resolve, reject) => {
        zlib.gunzip(Buffer.from(data, "base64"), GZIP_PARAMS, (err, result) => {
            if (!err) resolve(result.toString());
            reject(err);
        });
    });
}


var controller = {
    index: function (req, res) {
        return res.status(200).render('index', { title: 'Express', page: 'index' });
    },
    createData: async function (req, res) {
        //createFacturas.createData;
        res.send("OK");
    },
    insertFactura: function (req, res) {
        return res.status(200).render(
            'insert',
            {
                title: 'Inserción de Facturas',
                page: 'insercion'
            }
        );
    },
    insertManyView: async function (req, res) {
        res.status(200).render('insertMany', {
            title: 'Inserción múltiple de facturas',
            page: 'insertMany'
        });
    },
    statistics: function (req, res) {
        res.status(200).render('showStatistics', {
            title: 'Estadísticas por Sector',
            page: 'estadisticas'
        });
    },
    showStatistics: function (req, res) {
        let sector = req.query.sector;
        let nif = req.query.nif;

        switch (sector) {
            case "hosteleria":

                res.status(200).send(estadisticasHosteleria(nif));
                //res.status(200).send(pruebasEstadisticasHosteleria(nif));
                break;
            case "maquinaria":
                res.status(200).send(estadisticasMaquinaria(nif));
                break;
            default:
                res.status(400).send("Incorrent Query");
                break;
        }
    },
    insertMany: async function (req, res) {

        var num_agrupadas = req.query.num;
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

        var tbai_list = [];
        var agrupacion = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_1.xml").toString();
        tbai_list.push(DATA.getIdentTBAI(agrupacion));

        var nif = DATA.getNif(agrupacion);
        var fecha_inicio_agrupacion = DATA.getFechaExp(agrupacion);
        var fecha_fin_agrupacion = moment(fecha_inicio_agrupacion, "DD-MM-YYYY").add(7, "d").format("DD-MM-YYYY");

        for (var i = 2; i < num_agrupadas; i++) {
            let factura = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_" + i + ".xml").toString();
            agrupacion += factura;
            tbai_list.push(DATA.getIdentTBAI(factura));
        }

        var compresion_start = performance.now();
        var agrupacion_compress = await compressData(agrupacion);
        var compresion_fin = performance.now();

        const insert_query = "insert into facturas_agrupadas (nif, fecha_inicio, agrupacion, fecha_fin, tbai_id_list) values (?,?,?,?,?)";
        const params = [
            nif,
            moment(fecha_inicio_agrupacion, "DD-MM-YYYY").format("YYYY-MM-DD"),
            agrupacion_compress,
            moment(fecha_fin_agrupacion, "DD-MM-YYYY").format("YYYY-MM-DD"),
            tbai_list
        ];
        var insertar_cassandra_start = performance.now();
        await client.execute(insert_query, params, { prepare: true });
        var insert_cassandra_fin = performance.now();

        /**Comprobación de particiones para insertar en Mongo */

        var data_to_insert = {};
        data_to_insert.nif = nif;
        data_to_insert.fechaInicio = moment(fecha_inicio_agrupacion, "DD-MM-YYYY").toDate();
        data_to_insert.fechaFin = moment(fecha_fin_agrupacion, "DD-MM-YYYY").toDate();
        data_to_insert.idents = tbai_list;
        data_to_insert.agrupacion = agrupacion_compress;

        let numParticiones = 0;
        //BYTES DE TODO EL DOCUMENTO A INSERTAR EN LA BD (NO PUEDE SUPERAR LOS 16MB)
        let bytes = new TextEncoder().encode(JSON.stringify(data_to_insert)).byteLength;
        //CALCULO EL NUMERO DE PARTICIONES DEL DOCUMENTO
        if (bytes % (15 * MB) == 0) {
            numParticiones = Math.floor(bytes / (15 * MB));
        } else {
            numParticiones = 1 + Math.floor(bytes / (15 * MB));
        }
        var insert_array = [];
        var comprimir_mongo = [];
        if (numParticiones == 1) {
            insert_array.push(data_to_insert);
            comprimir_mongo.push(compresion_fin - compresion_start);
        } else {
            for (var j = 0; j < numParticiones; j++) {
                var agrupacion_mongo = "";
                var tbai_part_list = [];
                for (var k = Math.round(((j * num_agrupadas) / numParticiones)) + 1; k <= Math.round((j + 1) * num_agrupadas) / numParticiones; k++) {
                    let factura = fs.readFileSync(FACTURAS_AGRUPADAS_PATH + "grupo_" + num_agrupadas + "_" + k + ".xml").toString();
                    agrupacion_mongo += factura;
                    tbai_part_list.push(DATA.getIdentTBAI(factura));
                }
                var compress_mongo_start = performance.now();
                let agrupacion_mongo_compress = await compressData(agrupacion_mongo);
                var compress_mongo_fin = performance.now();
                let new_data_to_insert = {};
                new_data_to_insert.nif = nif;
                new_data_to_insert.fechaInicio = moment(fecha_inicio_agrupacion, "DD-MM-YYYY").toDate();
                new_data_to_insert.fechaFin = moment(fecha_fin_agrupacion, "DD-MM-YYYY").toDate();
                new_data_to_insert.idents = tbai_part_list;
                new_data_to_insert.agrupacion = agrupacion_mongo_compress;

                insert_array.push(new_data_to_insert);
                comprimir_mongo.push(compress_mongo_fin - compress_mongo_start);
            }
        }

        var insert_mongo_start = performance.now();
        await insert_agrupadas_mongo(insert_array);
        var insert_mongo_fin = performance.now();

        res.status(200).send({
            tbai_id: tbai_list[tbai_list.length - 1],
            stats: {
                insert_cassandra: insert_cassandra_fin - insertar_cassandra_start,
                insert_mongo: insert_mongo_fin - insert_mongo_start,
                comprimir_total_cassandra: compresion_fin - compresion_start,
                comprimir_mongo: comprimir_mongo
            }
        });


    },
    tctest: async function (req, res) {
        var num = req.query.num;
        if (num == null || num == 0 || num > 5000) {
            num = 100;
        }

        var gzip_time_list = [];
        var brotli_time_list = [];
        var lzma_time_list = [];
        var lzss_time_list = [];

        var gzip_decompress_time_list = [];
        var brotli_decompress_time_list = [];
        var lzma_decompress_time_list = [];
        var lzss_decompress_time_list = [];

        var gzip_ratio_list = [];
        var brotli_ratio_list = [];
        var lzma_ratio_list = [];
        var lzss_ratio_list = [];


        var labels = [];
        for (var i = 1; i <= num; i++) {
            let factura = fs.readFileSync('./facturas/factura_' + i + '.xml').toString();
            let bytes_start = new TextEncoder().encode(factura).byteLength;

            //GZIP
            var gzip_compresion_start = performance.now();
            let compress_gzip = await zlib.gzipSync(factura, { level: 1 });
            var gzip_compresion_fin = performance.now();
            var gzip_decompresion_start = performance.now();
            let decompress_gzip = await zlib.gunzipSync(compress_gzip);
            var gzip_decompresion_fin = performance.now();

            //BROTLI
            var brotli_compresion_start = performance.now();
            let compress_broli = await zlib.brotliCompressSync(factura);
            var brotli_compresion_fin = performance.now();
            var brotli_decompresion_start = performance.now();
            let decompress_broli = await zlib.brotliDecompressSync(compress_broli);
            var brotli_decompresion_fin = performance.now();

            //LZMA
            var lzma_compresion_start = performance.now();
            let compress_lzma = await compress_lzma_file(factura);
            var lzma_compresion_fin = performance.now();
            var lzma_decompresion_start = performance.now();
            let decompress_lzma = await decompress_lzma_file(compress_lzma);
            var lzma_decompresion_fin = performance.now();

            //LZSS
            var lzss_compresion_start = performance.now();
            var compress_lzss = await lzss.compress(factura);
            var lzss_compresion_fin = performance.now();
            var lzss_decompresion_start = performance.now();
            var decompress_lzss = await lzss.decompress(compress_lzss);
            var lzss_decompresion_fin = performance.now();


            gzip_time_list.push(gzip_compresion_fin - gzip_compresion_start);
            brotli_time_list.push(brotli_compresion_fin - brotli_compresion_start);
            lzma_time_list.push(lzma_compresion_fin - lzma_compresion_start);
            lzss_time_list.push(lzss_compresion_fin - lzss_compresion_start);

            gzip_decompress_time_list.push(gzip_decompresion_fin - gzip_decompresion_start);
            brotli_decompress_time_list.push(brotli_decompresion_fin - brotli_decompresion_start);
            lzma_decompress_time_list.push(lzma_decompresion_fin - lzma_decompresion_start);
            lzss_decompress_time_list.push(lzss_decompresion_fin - lzss_decompresion_start);

            gzip_ratio_list.push(1 - (compress_gzip.byteLength / bytes_start));
            brotli_ratio_list.push(1 - (compress_broli.byteLength / bytes_start));
            lzma_ratio_list.push(1 - (Buffer.byteLength(compress_lzma) / bytes_start));
            lzss_ratio_list.push(1 - (Buffer.byteLength(compress_lzss) / bytes_start));

            labels.push(i);
        }//End For




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

        let script_decom = ' <script> ' +
            'var ctx_decom = document.getElementById("decompress_time_chart");' +
            'const decom_labels_time = ["' + labels.join('\","') + '"];' +
            'const decom_data_time = {' +
            'labels: decom_labels_time,' +
            'datasets : [{' +
            'label: "GZip",' +
            'data: [' + gzip_decompress_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(75, 192, 192)",' +
            'tension: 0.1},' +
            '{label: "Brotli",' +
            'data: [' + brotli_decompress_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(255, 0, 0)",' +
            'tension: 0.1},' +
            '{label: "LZMA",' +
            'data: [' + lzma_decompress_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(0, 255, 0)",' +
            'tension: 0.1},' +
            '{label: "LZSS",' +
            'data: [' + lzss_decompress_time_list.toString() + '],' +
            'fill:false,' +
            'borderColor: "rgb(255, 255, 0)",' +
            'tension: 0.1}' +
            ']' +
            '};' +

            'const decom_config_time = {' +
            'type: "line",' +
            'data: decom_data_time' +
            '};' +
            'var decom_chart = new Chart(ctx_decom, decom_config_time);</script>';

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


        res.status(200).render('tctest', {
            title: 'Inserción de Facturas',
            page: 'tctest',
            script_time: script_time,
            script_ratio: script_ratio,
            script_decom: script_decom
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
                '$("#agrupadas-chart").hide();' +
                'if(Chart.getChart("get_factura_chart") != null){' +
                'Chart.getChart("get_factura_chart").destroy();}' +
                'if(Chart.getChart("descompresion-chart") != null){' +
                'Chart.getChart("descompresion-chart").destroy();}' +
                'if(Chart.getChart("recuperacion-agrupacion-chart") != null){' +
                'Chart.getChart("recuperacion-agrupacion-chart").destroy();}' +
                'var ctx = document.getElementById("get_factura_chart");' +
                'const labels = ["MongoDB", "Cassandra"];' +
                'const data = {' +
                'labels: labels,' +
                'datasets:[{' +
                'label: "Tiempo de Obtención de Datos (milisegundos)",' +
                'data: [' + (result_mongo.stats.busqueda_datos) + ',' + (result_cassandra.stats.busqueda_datos) + '],' +
                'backgroundColor: ["rgba(255, 99, 132, 0.2)", "rgba(54, 162, 235, 0.2)"],' +
                'borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],' +
                'borderWidth: 1' +
                '}]' +
                '};' +

                'const config = {' +
                'type: "bar",' +
                'data: data' +
                '};' +
                'var chart = new Chart(ctx, config);</script>';
            var script_decom = "";
            var script_busqueda_fact = "";
            if (result_mongo.agrupada) {
                script_decom = ' <script> ' +
                    '$("#agrupadas-chart").show();' +
                    'var ctx_decom = document.getElementById("descompresion-chart");' +
                    'const labels_decom = ["MongoDB", "Cassandra"];' +
                    'const data_decom = {' +
                    'labels: labels_decom,' +
                    'datasets:[{' +
                    'label: "Tiempo de descompresión (milisegundos)",' +
                    'data: [' + (result_mongo.stats.descompresion) + ',' + (result_cassandra.stats.descompresion) + '],' +
                    'backgroundColor: ["rgba(255, 99, 132, 0.2)", "rgba(54, 162, 235, 0.2)"],' +
                    'borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],' +
                    'borderWidth: 1' +
                    '}]' +
                    '};' +

                    'const config_decom = {' +
                    'type: "bar",' +
                    'data: data_decom' +
                    '};' +
                    'var chart = new Chart(ctx_decom, config_decom);</script>';
                script_busqueda_fact = ' <script> ' +
                    'var ctx_busqueda_fact = document.getElementById("recuperacion-agrupacion-chart");' +
                    'const labels_busqueda_fact = ["MongoDB", "Cassandra"];' +
                    'const data_busqueda_fact = {' +
                    'labels: labels_busqueda_fact,' +
                    'datasets:[{' +
                    'label: "Tiempo de Búsqueda en la Agrupación (milisegundos)",' +
                    'data: [' + (result_mongo.stats.busqueda_factura) + ',' + (result_cassandra.stats.busqueda_factura) + '],' +
                    'backgroundColor: ["rgba(255, 99, 132, 0.2)", "rgba(54, 162, 235, 0.2)"],' +
                    'borderColor: ["rgb(255, 99, 132)","rgb(54, 162, 235)"],' +
                    'borderWidth: 1' +
                    '}]' +
                    '};' +

                    'const config_busqueda_fact = {' +
                    'type: "bar",' +
                    'data: data_busqueda_fact' +
                    '};' +
                    'var chart = new Chart(ctx_busqueda_fact, config_busqueda_fact);</script>';
            }


            res.status(200).render('gr', {
                title: 'Búsqueda de Facturas',
                page: 'getFactura',
                error: '<script>document.getElementById("data").style.display = "block";</script><script>document.getElementById("tbai_error").style.display = "none";</script>',
                tbai_id: result_mongo.data.tbai_id,
                nif_emisor: result_mongo.data.nif_emisor,
                serie_factura: result_mongo.data.serie_factura,
                num_factura: result_mongo.data.num_factura,
                importe_factura: result_mongo.data.importe_factura + " €",
                fecha_exp: moment(result_mongo.data.fecha_exp, "DD-MM-YYYY").format("YYYY/MM/DD"),
                script: script,
                script_decom: script_decom,
                script_busqueda_fact: script_busqueda_fact
            });
        }



    },
    insercionFacturas: async function (req, res) {//Compresión y descompresión del fichero
        //Tengo que comprimir el fichero con las distintas tecnicas de compresión,
        //descomprimirlo y devolver un json con los resultados.

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
                let resul = await insert_mongo(json).catch((err) => {
                    return err.code;
                }).then((err) => {
                    console.log(err);
                    if (err) {
                        return err;
                    }
                    return "OK";
                });

                if (resul != "OK") {//Ha ocurrido algun tipo de error
                    fs.unlinkSync(filePath);
                    return res.status(200).send(
                        {
                            title: 'Inserción de Facturas',
                            page: 'insertFacturas',
                            tbai_id: resul
                            //file: compressed
                        }
                    );
                }
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
                fs.unlinkSync(filePath);
                res.status(200).send({
                    tbai_id: json._id,
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas'
                });


            } else {//Error el formato no es correcto
                res.status(200).send({
                    tbai_id: -2,
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas'
                });
            }

        } else {//error al enviar los archivos
            res.status(200).send(
                {
                    title: 'Inserción de Facturas',
                    page: 'insertFacturas',
                    tbai_id: -3
                }
            );
        }
    }, insertFacturasEstadisticas: async function (req, res) {

        //const DIRECTORY_PATH = "/Users/gorkaalvarez/Desktop/Uni/tbaiData/";
        const DIRECTORY_PATH = "C:\\Users\\877205\\Desktop\\FacturasInsert\\insertData\\";
        await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
        const index = fs.readFileSync(DIRECTORY_PATH + "index.txt").toString().split("\n");
        for (var i = 0; i < index.length; i++) {
            //let nif = companies_nif_list[i][0];
            let file = index[i].split("/")[5];
            //let file = index[i];
            //console.log(DIRECTORY_PATH+file);
            try {
                var facturas = JSON.parse(fs.readFileSync(DIRECTORY_PATH + file).toString());
                //await insert_mongo(facturas).then(() => {console.log("Guardada --> "+file)}).catch(() => {console.log("Error al guardar --> "+file)});
            } catch (err) {
                console.log("Error al leer la factura --> " + file);
            }

            var array = [];
            for (var j = 0; j < facturas.length; j++) {
                let factura_j = facturas[j];
                let data = {};
                data._id = factura_j.id_tbai;
                data.nif = factura_j.nif;
                data.fecha = moment(factura_j.fecha).toDate();
                data.cantidad = factura_j.cantidad;
                data.serie = factura_j.serie;
                data.status = factura_j.status;
                data.xml = factura_j.xml;
                array.push(data);
            }
            await insert_mongo(array).then(() => { console.log("Guardada --> " + file) }).catch(() => { console.log("Error al guardar --> " + file) });


        }

        res.status(200).send("OK");

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

        var busqueda_bd_start = performance.now();
        client.execute(query_indiv, params_indiv, { prepare: true }).then((resul) => {
            if (resul.rowLength < 1) {
                const query_gr = "select fecha_fin, tbai_id_list, agrupacion from facturas_agrupadas where nif=? and fecha_inicio <= ?";
                const params_gr = [
                    tbai_id.split("-")[1],
                    moment(tbai_id.split("-")[2], "DDMMYY").format("YYYY-MM-DD")
                ];
                client.execute(query_gr, params_gr, { prepare: true }).then((res) => {
                    var agrupacion = "";
                    var tbai_list;
                    for (var i = 0; i < res.rowLength; i++) {
                        if (moment(res.rows[i].fecha_fin, "YYYY-MM-DD").toDate() >= moment(tbai_id.split("-")[2], "DDMMYY").toDate()) {
                            agrupacion = res.rows[i].agrupacion;
                            tbai_list = res.rows[i].tbai_id_list;
                            break;
                        }
                    }
                    var busqueda_bd_fin = performance.now();
                    var descompresion_start = performance.now();
                    unCompressData(agrupacion).then((resul) => {
                        var descompresion_fin = performance.now();
                        var busqueda_factura_start = performance.now();

                        var pos = Array.from(tbai_list).indexOf(tbai_id);
                        var facturas_array = resul.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                        let data = facturas_array[pos];

                        var busqueda_factura_fin = performance.now();

                        resolve({
                            code: 200,
                            agrupada: true,
                            data: {
                                tbai_id: tbai_id,
                                nif_emisor: DATA.getNif(data),
                                serie_factura: DATA.getSerieFactura(data),
                                num_factura: DATA.getNumFactura(data),
                                importe_factura: DATA.getImporteTotalFactura(data),
                                fecha_exp: DATA.getFechaExp(data)
                            },
                            stats: {
                                busqueda_datos: busqueda_bd_fin - busqueda_bd_start,
                                descompresion: descompresion_fin - descompresion_start,
                                busqueda_factura: busqueda_factura_fin - busqueda_factura_start
                            }
                        });
                    });


                });
            } else {
                var busqueda_bd_fin = performance.now();

                resolve({
                    agrupada: false,
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
                            var agrupacion = docs[i].agrupacion;
                            var descompresion_start = performance.now();

                            unCompressData(agrupacion).then((resul) => {
                                var descompresion_fin = performance.now();

                                var busqueda_factura_start = performance.now();
                                var facturas_array = resul.split(/(?=\<\?xml version="1\.0" encoding="utf-8"\?\>)/);
                                let data = facturas_array[pos];
                                var busqueda_factura_fin = performance.now();


                                resolve({
                                    code: 200,
                                    agrupada: true,
                                    data: {
                                        tbai_id: tbai_id,
                                        nif_emisor: DATA.getNif(data),
                                        serie_factura: DATA.getSerieFactura(data),
                                        num_factura: DATA.getNumFactura(data),
                                        importe_factura: DATA.getImporteTotalFactura(data),
                                        fecha_exp: DATA.getFechaExp(data)
                                    },
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
                    agrupada: false,
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

function executeQuery(query) {
    return new Promise((resolve) => {
        Factura.find(query, (err, result) => {
            if (!err) resolve(result);
        });
    });
}


async function createEstadisticaDiaria(nif) {
    await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    console.log("Estadistica diaria");
    try {
        console.log("Dentro del try");
        if (fs.existsSync("./estadisticas/2021-03-19_2021-03-19_global.txt")) {//Si existe la de un nif entonces existe la global
            console.log("Existe fichero?");
            var global_dia_estadistica = fs.readFileSync("./estadisticas/2021-03-19_2021-03-19_global.txt").toString().split("\n");
            if (global_dia_estadistica.map(l => l.split(" // ")[0]).filter(n => d == `dia_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                let query_dia_result = await Factura.find({
                    $and: [
                        { nif: nif },
                        {
                            fecha: {
                                $gte: new Date("2021-03-19T00:00:00")
                            }
                        },
                        {
                            fecha: {
                                $lte: new Date("2021-03-19T23:59:59")
                            }
                        }
                    ]
                }, "nif fecha cantidad xml").exec();

                var dia_nif = [];
                var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                for (var t = moment("00:00:00", "HH:mm:ss").toDate(); t <= moment("23:00:00", "HH:mm:ss").toDate(); t = moment(t).add(1, "hours").toDate()) {
                    let t_aux = t;
                    let nif_array = query_dia_descomp.filter(f => moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t && moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate());
                    let nif_average = nif_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / global_array.length;

                    dia_nif.push(nif_average);
                }
                fs.appendFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_${nif} // [${dia_nif.toString()}]\n`);
            }//end if
        } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
            console.log("No existe fichero");
            var nif_list = companies_nif_list.map(c => c[0]).slice(0, 763);
            let query_dia_result = await Factura.find({
                $and: [
                    {
                        nif: {
                            $in: nif_list
                        }
                    },
                    {
                        fecha: {
                            $gte: new Date("2021-03-19T00:00:00")
                        }
                    },
                    {
                        fecha: {
                            $lte: new Date("2021-03-19T23:59:59")
                        }
                    }
                ]
            }, "nif fecha cantidad xml").exec();
            var dia_labels = [];
            var dia_nif = [];
            var dia_sector = [];
            var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
            for (var t = moment("00:00:00", "HH:mm:ss").toDate(); t <= moment("23:00:00", "HH:mm:ss").toDate(); t = moment(t).add(1, "hours").toDate()) {
                let t_aux = t;
                let global_array = query_dia_descomp.filter(f => (moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t) && (moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate()));
                let global_average = global_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / global_array.length;
                //console.log(global_array);

                let nif_array = query_dia_descomp.filter(f => DATA.getNif(f) == nif).filter(f => moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t && moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate());
                let nif_average = nif_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / global_array.length;

                dia_nif.push(nif_average);
                dia_sector.push(global_average);
                dia_labels.push(moment(t).format("HH:mm:ss"));
                console.log(dia_labels);
            }
            fs.writeFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_labels // [${dia_labels.toString()}]\ndia_sector // [${dia_sector.toString()}]\ndia_${nif} // [${dia_nif.toString()}]\n`);
        }//end if
    } catch (err) {

        console.log(err);
    }
}

async function createEstadisticaSemanal(nif) {
    await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    console.log("Estadistica Semanal");
    try {
        console.log("Dentro del try");
        if (fs.existsSync("./estadisticas/2021-03-19_2021-03-19_global.txt")) {//Si existe la de un nif entonces existe la global
            console.log("Existe fichero?");
            var global_dia_estadistica = fs.readFileSync("./estadisticas/2021-03-19_2021-03-19_global.txt").toString().split("\n");
            if (global_dia_estadistica.map(l => l.split(" // ")[0]).filter(n => d == `dia_${nif}`).length == 0) {//No existe la estadistica sobre ese nif
                let query_dia_result = await Factura.find({
                    $and: [
                        { nif: nif },
                        {
                            fecha: {
                                $gte: new Date("2021-03-19T00:00:00")
                            }
                        },
                        {
                            fecha: {
                                $lte: new Date("2021-03-19T23:59:59")
                            }
                        }
                    ]
                }, "nif fecha cantidad xml").exec();

                var dia_nif = [];
                var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
                for (var t = moment("00:00:00", "HH:mm:ss").toDate(); t <= moment("23:00:00", "HH:mm:ss").toDate(); t = moment(t).add(1, "hours").toDate()) {
                    let t_aux = t;
                    let nif_array = query_dia_descomp.filter(f => moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t && moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate());
                    let nif_average = nif_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / global_array.length;

                    dia_nif.push(nif_average);
                }
                fs.appendFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_${nif} // [${dia_nif.toString()}]\n`);
            }//end if
        } else {//No existe la estadistica global ni la del nif, asi que tengo que crearlas
            console.log("No existe fichero");
            var nif_list = companies_nif_list.map(c => c[0]).slice(0, 763);
            let query_dia_result = await Factura.find({
                $and: [
                    {
                        nif: {
                            $in: nif_list
                        }
                    },
                    {
                        fecha: {
                            $gte: new Date("2021-03-19T00:00:00")
                        }
                    },
                    {
                        fecha: {
                            $lte: new Date("2021-03-19T23:59:59")
                        }
                    }
                ]
            }, "nif fecha cantidad xml").exec();
            var dia_labels = [];
            var dia_nif = [];
            var dia_sector = [];
            var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
            for (var t = moment("00:00:00", "HH:mm:ss").toDate(); t <= moment("23:00:00", "HH:mm:ss").toDate(); t = moment(t).add(1, "hours").toDate()) {
                let t_aux = t;
                let global_array = query_dia_descomp.filter(f => (moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t) && (moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate()));
                let global_average = global_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / global_array.length;
                //console.log(global_array);

                let nif_array = query_dia_descomp.filter(f => DATA.getNif(f) == nif).filter(f => moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() >= t && moment(DATA.getHoraExpedionFactura(f), "HH:mm:ss").toDate() < moment(t_aux).add(1, "hours").toDate());
                let nif_average = nif_array.map(f => DATA.getImporteTotalFactura(f)).reduce((a, b) => a + b, 0) / global_array.length;

                dia_nif.push(nif_average);
                dia_sector.push(global_average);
                dia_labels.push(moment(t).format("HH:mm:ss"));
                console.log(dia_labels);
            }
            fs.writeFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt', `dia_labels // [${dia_labels.toString()}]\ndia_sector // [${dia_sector.toString()}]\ndia_${nif} // [${dia_nif.toString()}]\n`);
        }//end if
    } catch (err) {

        console.log(err);
    }
}

function estadisticasHosteleria(nif) {

    createEstadisticaDiaria(nif);

    var file = fs.readFileSync('./estadisticas/2021-03-19_2021-03-19_global.txt').toString().split('\n');
    var dia_labels = JSON.stringify(file[0].split(" // ")[1]);
    var dia_sector = JSON.stringify(file[1].split(" // ")[1]);

    var dia_nif = JSON.stringify(file.filter(l => l.split(" // ")[0] == `dia_${nif}`)[0].split(" // ")[1]);

    return {
        dia_nif: dia_nif,
        dia_sector: dia_sector,
        dia_labels: dia_labels
    }

    //var nif_list = companies_nif_list.map(c => c[0]).slice(0,763);
    //console.log(nif_list.toString());
    //let dia_time_start = performance.now();


    //query_dia_result.map(f => console.log(f.fecha));
    //console.log(performance.now() - dia_time_start);
    /*let semana_time_start = performance.now();
    let query_semana_result = await Factura.find({
        "nif" : {
            "$in" : nif_list
        },
        "fecha" : {
            "$lte": "2021-03-15"
        },
        "fecha":{
            "$gte": "2021-03-21"
        }
    },
    {
        "xml" : 1,
        "_id":0
    }).exec();

    console.log(performance.now() - semana_time_start);
    let mes_time_start = performance.now();
    let query_mes_result = await Factura.find({
        "nif" : {
            "$in" : nif_list
        },
        "fecha" : {
            "$lte": "2021-03-01"
        },
        "fecha":{
            "$gte":"2021-03-28"
        }
    },
    {
        "xml" : 1,
        "_id":0
    }).exec();
    console.log(performance.now() - mes_time_start);
    let trimestre_time_start = performance.now();
    let query_trimestre_result = await Factura.find({
        "nif" : {
            "$in" : nif_list
        },
        "fecha" : {
            "$lte": "2021-01-04"
        },
        "fecha":{
            "$gte":"2021-03-28"
        }
    },
    {
        "xml" : 1,
        "_id":0
    }).exec();
    console.log(performance.now() - trimestre_time_start);

*/

    var semana_labels = [];
    var mes_labels = [];
    var trimestre_labels = [];



    //query_dia_result.sort((a,b) => moment(a.fecha).format("HH:mm:ss") > moment(b.fecha).format("HH:mm:ss") ? 1 : -1);

    /*for(var i = 0; i < query_dia_result.length; i++){
        dia_labels.push(moment(query_dia_result[i].fecha).format("HH:mm:ss"));
        if(query_dia_result[i].nif == nif){
            dia_nif.push(query_dia_result[i].cantidad);
        }


    }*/
    //console.log(query_dia_result[0]);
    //var query_dia_descomp = query_dia_result.map(f => zlib.gunzipSync(Buffer.from(f.xml, "base64"), GZIP_PARAMS).toString());
    //console.log(query_dia_descomp[0]);
    //console.log("Todo descomprimido");



    //console.log(dia_sector);
    //console.log(dia_nif);
    //console.log(dia_labels);
    /*return {
        dia_nif: dia_nif,
        dia_sector: dia_sector,
        dia_labels: dia_labels
    }*/



}

async function pruebasEstadisticasHosteleria(nif) {
    await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    //nif = "00676565C";

    //fs.writeFileSync("./files/estadisticas_hosteleria.csv", "NIF;ObtenerDatos;DescomprimirDatos;BusquedaRAW;BusquedaBD;Descomprimir;Sumar\n",{flag: "w"} );


    /**QUERY_TODAS_LAS_FACTURAS */
    var obtener_facturas_start = performance.now();
    let query_1_result = await executeQuery({ nif: nif });
    var obtener_facturas_fin = performance.now();

    var descompresion_start = performance.now();
    for (var i = 0; i < query_1_result.length; i++) {
        let factura_descomp = await unCompressData(query_1_result[i].xml).then((res) => {/*console.log("Descompresion realizada")*/ }).catch((err) => { throw err });
        //fs.writeFileSync('./files/gzip.txt', Buffer.from(query_1_result[i].xml, "base64").toString("hex"));
        //let factura_descomp = zlib.gunzipSync(Buffer.from(query_1_result[i].xml, "base64"));
    }

    var descompresion_fin = performance.now();


    console.log("Tiempo en Recuperar de BD --> " + (obtener_facturas_fin - obtener_facturas_start));
    console.log("Tiempo en descomprimir todo --> " + (descompresion_fin - descompresion_start));

    /** QUERY SUMA TOTAL FACTURAS CON FILTRO EN RAW */

    var busqueda_facturas_filtro_start = performance.now();
    await Factura.aggregate([
        {
            "$match": {
                "nif": nif,
                "cantidad": {
                    "$gte": 10
                }
            }
        },
        {
            "$group": {
                "_id": {

                },
                "SUM(cantidad)": {
                    "$sum": "$cantidad"
                }
            }
        }
    ]).exec();

    var busqueda_facturas_filtro_fin = performance.now();

    var obtener_facturas_2_start = performance.now();
    let query_2_result = await executeQuery({ nif: nif });
    var obtener_facturas_2_fin = performance.now();

    var array_facturas_descomp = [];
    var descomprimir_2_start = performance.now();
    for (var i = 0; i < query_2_result.length; i++) {
        let factura_descomp = await unCompressData(query_2_result[i].xml).catch((err) => { console.log("Error al descomprimir en query_2") });
        //let json = parser.xml2json(factura_descomp, {compact: true, ignoreAttributes: true, ignoreDeclaration: true, spaces: '\t'});
        array_facturas_descomp.push(factura_descomp);
    }
    var descomprimir_2_fin = performance.now();

    //console.log(array_facturas_descomp[0]);
    var sumar_start = performance.now();
    let suma = array_facturas_descomp.map(f => DATA.getImporteTotalFactura(f)).filter(i => i <= 10).reduce((a, b) => a + b, 0);
    var sumar_fin = performance.now();
    console.log(suma);
    fs.writeFileSync("./files/estadisticas_hosteleria.csv", nif + ";" + (obtener_facturas_fin - obtener_facturas_start) + ";" + (descompresion_fin - descompresion_start) + ";" + (busqueda_facturas_filtro_fin - busqueda_facturas_filtro_start) + ";" + (obtener_facturas_2_fin - obtener_facturas_2_start) + ";" + (descomprimir_2_fin - descomprimir_2_start) + ";" + (sumar_fin - sumar_start) + "\n", { flag: "a" });
    console.log("OK");

}


module.exports = controller;