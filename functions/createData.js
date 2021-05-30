'use strict'

const fs = require('fs');
const zlib = require('zlib');

var Factura = require('../model/factura');
var AgrupacionFactura = require('../model/facturaAgrupada');
const DATA = require('../functions/getData');
var data_generator = require('../functions/data_generator');
const transformer = require('../functions/transformer');
const companies_nif_list = require('../functions/companies_nif').companies_nif_list;
const nif_list = require('../functions/nif_list').nif_list;
const moment = require('moment');
const { randomInt } = require('crypto');
var SignedXml = require('xml-crypto').SignedXml;
var select = require('xml-crypto').xpath;
var dom = require('xmldom').DOMParser;
var FileKeyinfo = require('xml-crypto').FileKeyInfo;

const mongoUrl = "mongodb://localhost:27017";
const dbName = 'ticketbai';

function compressData(data) {
    return new Promise((resolve) => {
        zlib.gzip(data, { level: 1 }, (err, result) => {
            if (!err) resolve(result.toString('base64'),
            );
        });
    });
}

function getRandomInt(min, max) {
    return Number(Math.floor(Math.random() * (max - min)) + min);
}


function generarFactura(nif_name, fecha){
    return new Promise((resolve, reject) => {
        var privateKey = fs.readFileSync('./keys/user1.pem');
        var sig = new SignedXml();
        sig.addReference("//*[local-name(.)='Cabecera' or local-name(.) = 'Sujetos' or local-name(.) = 'Factura' or local-name(.) = 'HuellaTBAI']");
        sig.signingKey = privateKey;
    
        var prob = getRandomInt(1,101);
        var detalles = 0;
        if(prob <= 80){
            detalles = getRandomInt(1,6);
        }else{
            detalles = getRandomInt(1,16);
        }
        var iva = 0;
        prob = getRandomInt(1,101);
        if(prob <= 50){
            iva = 10;
        }else{
            iva = 21;
        }

        var status;
        prob = getRandomInt(1,101);
        if(prob <= 5){
            prob = getRandomInt(1,101);
            if(prob <= 50){
                status = -1;
            }else{
                status = 1;
            }
        }else{
            status = 0;
        }
    
        prob = getRandomInt(1,101);
        var hour;
        if(prob <= 80){//sobre las 9 / 15 / 20
            prob = getRandomInt(0,3);
            switch (prob) {
                case 0: //Entre las 8:30 y 9:30
                    hour = moment("08:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 121);
                    hour.add(mins, "minutes");
                    break;
                case 1: //las 14:30 y 15:30
                    hour = moment("14:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 121);
                    hour.add(mins, "minutes");
                    break;
                case 2: //las 19:30 y 20:30
                    hour = moment("19:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 121);
                    hour.add(mins, "minutes");
                    break;
                default:
                    break;
            }
        }else{
            prob = getRandomInt(0,4);
            switch (prob) {
                case 0: //Entre las 6:30 y 8:30
                    hour = moment("06:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 121);
                    hour.add(mins, "minutes");
                    break;
                case 1: //las 9:30 y 14:30
                    hour = moment("09:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 301);
                    hour.add(mins, "minutes");
                    break;
                case 2: //las 15:30 y 19:30
                    hour = moment("15:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 241);
                    hour.add(mins, "minutes");
                    break;
                case 3: //las 20:30 y 00:00
                    hour = moment("20:30:00", "HH:mm:ss");
                    var mins = getRandomInt(0, 211);
                    hour.add(mins, "minutes");
                    break;
                default:
                    break;
            }
        }
    
    
        data_generator.sujetos_config.nif = nif_name;
        data_generator.cabecera_factura_config.serieFactura = nif_name[0]+moment(fecha).format("DDMMYYHHmmss");
        data_generator.cabecera_factura_config.NumFactura = getRandomInt(0,1000001);
        data_generator.cabecera_factura_config.FechaExpedicionFactura = moment(fecha).format("DD-MM-YYYY");
        data_generator.cabecera_factura_config.HoraExpedicionFactura = moment(hour).format("HH:mm:ss");
        data_generator.datos_factura_config.detallesFactura.numDetalles = detalles;
        data_generator.datos_factura_config.detallesFactura.minImporteUnitario = 0.5;
        data_generator.datos_factura_config.detallesFactura.maxImporteUnitario = 4;
        data_generator.datos_factura_config.detallesFactura.tipoImpositivo = iva;
    
        let data = data_generator.generate(data_generator.sujetos_config, data_generator.cabecera_factura_config, data_generator.datos_factura_config, data_generator.huellaTBAI_config);
        let xml = transformer.generate(data);
        sig.computeSignature(xml);
        
        let factura = sig.getSignedXml();
    
        var json = {};
    
        json._id = DATA.getIdentTBAI(factura);
        json.nif = nif_name[0];
        json.fecha = moment(fecha).format("YYYY-MM-DD");
        json.cantidad = Number(DATA.getImporteTotalFactura(factura));
        json.serie = DATA.getSerieFactura(factura);
        json.num_factura = DATA.getNumFactura(factura);
        json.status = status;

        compressData(factura).then(res => {
            json.xml = res;
            resolve(json);
        }).catch(err => {
            reject(err);
        })
    });
}


async function createData(){
    //await mongoose.connect(mongoUrl + "/" + dbName).then(() => { console.log("Conexión a MongoDB realizada correctamente") });
    const SAVE_PATH = "/Users/gorkaalvarez/Desktop/Uni/tbaiData/";
    const MAX_NIF = 1000;
    var total_facturas = 1902445;
    for(var i = 501; i < MAX_NIF; i++){
        var facturas_array = [];
        const nif_name = companies_nif_list[i];
        for(var j = moment("2021-01-01"); j <= moment("2021-03-31"); j = moment(j).add(1, "days")){//Por cada uno de los dias
            //console.log(moment(j).format("YYYY-MM-DD"));
            var prob = getRandomInt(1,101);
            var num_facturas_dia;
            if(prob <= 70){
                num_facturas_dia = getRandomInt(80,101)
            }else{
                num_facturas_dia = getRandomInt(1,101);
            }
            //const num_facturas_dia = randomInt(1, 101);
            for(var k = 0; k < num_facturas_dia; k++){//Numero de facturas a generar ese dia
                var weekDay = j.isoWeekday();
                if(weekDay == 1 || weekDay == 2 || weekDay == 3){
                    if(getRandomInt(1,101) <= 80){
                        let factura = await generarFactura(nif_name, j);
                        facturas_array.push(factura);
                    }
                }else{
                    let factura = await generarFactura(nif_name, j);
                    facturas_array.push(factura);
                }
            }//end for
        }//end for
        total_facturas += facturas_array.length; 
        console.log(`NIF --> ${nif_name[0]} // Numero Facturas Emitidas --> ${facturas_array.length} // Total de Facturas --> ${total_facturas}`);
        //console.log(facturas_array);
        fs.appendFileSync(SAVE_PATH+"nif-facturas-emitidas.txt", `NIF --> ${nif_name[0]} // Numero Facturas Emitidas --> ${facturas_array.length}\n`);
        fs.appendFileSync(SAVE_PATH+`${nif_name[0]}.json`, JSON.stringify(facturas_array));
        fs.appendFileSync(SAVE_PATH+"index.txt", `${nif_name[0]}.json\n`);
    }//end for
}//end function


//exports.createData = createData;