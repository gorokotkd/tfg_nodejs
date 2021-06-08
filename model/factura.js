'use strict'

var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var FacturaSchema = Schema({
    _id : {
        type: Schema.Types.String,
        required: true
    },
    nif : {
        type: Schema.Types.String,
        required : true
    },
    fecha : {
        type: Schema.Types.Date,
        required : true
    },
    cantidad : {
        type : Schema.Types.Number,
        required: true
    },
    serie : {
        type: Schema.Types.String,
        required : true
    },
    num_factura : {
        type: Schema.Types.String,
        required : true
    },
    xml : {
        required : true,
        type : Schema.Types.String
    },
    status : {
        required : true,
        type: Schema.Types.Number
    }
});

module.exports = mongoose.model('Factura', FacturaSchema);