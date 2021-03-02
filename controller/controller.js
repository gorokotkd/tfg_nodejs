


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
        
    }

};


module.exports = controller;