import xlrd
import unicodecsv
import unicodedata
import pandas as pd
import os

# VERSION  DE xlrd COMPATIBLE '1.2.0', VERSIONES MAS NUEVAS NO COMPATIBLES

start_fil = 6  # Inicio del contenido importante del excel
end_fil = 31  # fin del contenido importante del excel


def list_bank(wsh):
    lista_banks = []
    for banks in wsh.col_values(0, 13):
        # cuando lleguemos a un espacio en blanco terminamos de agregar columnas a la lista
        if(banks == ''):
            break
        lista_banks.append(banks)
    end_fil = 13 + len(lista_banks)
    return end_fil


def excel_to_csv_activos_1(list_file_excel, location_excel, out_csv):

    print 'File: '+list_file_excel
    print 'Activos 1'

    # Ubicacion del Excel
    xls_filename = location_excel+list_file_excel

    workbook = xlrd.open_workbook(xls_filename)
    wsh = workbook.sheet_by_name('Activos Bancos 1')

    # Generar nombre de la Salida CSV
    csv_filename = xls_filename.rsplit('/', 1)[-1].rsplit('.', 1)[0]
    fileName = out_csv + "ab1" + csv_filename + ".csv"

    fh = open(fileName, "wb")
    csv_out = unicodecsv.writer(fh, encoding='UTF-8', delimiter=";")

    list_join_col = []
    lista_banks = []

    end_fil = list_bank(wsh)

    for row_number in range(start_fil, end_fil):

        list_join_col = []
        # Nos ubicamos en las cabeceras
        if(row_number == 6):
            # recorre las columnas de la fila seleccionada para eliminar los espacios en blanco entre columnas,
            # luego las juntamos todas para volver a formar la fila
            for col in wsh.row_values(6):
                if(col != ''):
                    list_join_col.append(col)

            # escribimos en el csv la fila completa
            csv_out.writerow(list_join_col)

        # Nos ubicamos en el fin del espacio entre las cabeceras y los datos
        elif(row_number >= 13):
            for col in wsh.row_values(row_number):
                if(col != ''):
                    list_join_col.append(col)
            # escribimos en el csv la fila completa
            csv_out.writerow(list_join_col)

    fh.close()
    print("CSV file created successfully: "+fileName)

# Combinar columnas de cada nivel, para Activos banco 2
# rellenaremos los espacios en blanco de las columnas por el valor que exista antes de llegar al espacio en blanco,
# ese espacio en blanco nos indica cuantas columnas de niveles inferiores tiene el valor que eleguimos para llenar los espacios.
# La parte una correspondiente a Adeudado por bancos (neto de provisiones) consta de 3 niveles de columnas


def excel_to_csv_activos_2(files, location_excel, out_csv):

    print 'File: ' + files
    print 'Activos 2'

    # Ubicacion del Excel
    xls_filename = location_excel+files
    print xls_filename

    workbook = xlrd.open_workbook(xls_filename)
    wsh = workbook.sheet_by_name('Activos Bancos 2')

    # Generar nombre de la Salida CSV
    csv_filename = xls_filename.rsplit('/', 1)[-1].rsplit('.', 1)[0]
    fileName = out_csv + "ab2" + csv_filename + ".csv"

    fh = open(fileName, "wb")
    csv_out = unicodecsv.writer(fh, encoding='UTF-8', delimiter=';')

    def complete_list(fil):
        # asignamos los espacios ocupados correspondiente a Adeudado por bancos (neto de provisiones)
        lista_temp = wsh.row_values(fil, 1, 11)
        valor = ''
        for elem in range(1, len(wsh.row_values(fil, 1, 11))):
            if(wsh.row_values(fil, 1, 11)[elem] == ''):
                valor = lista_temp[elem-1]
                lista_temp[elem] = valor

        return lista_temp

    list_temp = complete_list(7)  # fila 7
    list_temp2 = complete_list(8)  # fila 8

    # union fila 7 y 8 del excel ( nivel 1 y 2 de las columnas)
    n1_n2 = []

    for elem in range(0, len(wsh.row_values(7, 1, 11))):

        valor = 'Activos/'+list_temp[elem] + '/' + list_temp2[elem]

        n1_n2.append(valor)

    # union nivel 2 y 3 con fila 9
    n1_n2_n3_p1 = []
    for elem in range(0, len(n1_n2)):
        # si no existe otro nivel guardamos el nivel anterior solo sin unirla con nada
        if (wsh.row_values(9, 1, 11)[elem] == ''):
            valor = n1_n2[elem]
            n1_n2_n3_p1.append(valor)
        else:
            valor = n1_n2[elem] + '/' + wsh.row_values(9, 1, 11)[elem]
            n1_n2_n3_p1.append(valor)

    def complete_list2(fil, end):
        lista_temp = wsh.row_values(fil, end, 24)
        valor = ''
        for elem in range(1, len(wsh.row_values(fil, end, 24))):
            if(wsh.row_values(fil, end, 24)[elem] == ''):
                valor = lista_temp[elem-1]
                lista_temp[elem] = valor

        return lista_temp

    list_temp = complete_list2(7, 11)
    list_temp2 = complete_list2(8, 11)

    # union fila 7 y 8 del excel ( nivel 1 y 2)
    n1_n2 = []
    for elem in range(0, len(wsh.row_values(7, 11, 24))):
        valor = 'Activos/'+list_temp[elem] + '/' + list_temp2[elem]
        n1_n2.append(valor)

    list_temp = complete_list2(9, 11)
    # union nivel 2 y 3
    n1_n2_n3 = []
    for elem in range(0, len(n1_n2)):
        # si no existe otro nivel guardamos el nivel anterior solo sin unirla con nada
        if (list_temp[elem] == ''):
            valor = n1_n2[elem]
            n1_n2_n3.append(valor)
        else:
            valor = n1_n2[elem] + '/' + list_temp[elem]
            n1_n2_n3.append(valor)

    # union nivel 3 y 4
    n1_n2_n3_n4 = []
    for elem in range(0, len(n1_n2_n3)):
        # si no existe otro nivel guardamos el nivel anterior solo sin unirla con nada
        if (wsh.row_values(10, 11)[elem] == ''):
            valor = n1_n2_n3[elem]
            n1_n2_n3_n4.append(valor)
        else:
            valor = n1_n2_n3[elem] + '/' + wsh.row_values(10, 11)[elem]
            n1_n2_n3_n4.append(valor)

    last_col = wsh.ncols  # ultima columna del excel

    # ciclo para buscar la ultima columnas del excel y guardarla
    name_last_col = []
    for name_col in wsh.col_values(last_col-1, 1):
        if (name_col != ''):
            name_last_col.append(name_col)
            break

    # unimos las cabeceras de la union de la parte 1 y 2 y tambien las cabecera de las Instituciones y colocaciones
    header = wsh.row_values(6, 0, 1)+n1_n2_n3_p1+n1_n2_n3_n4+name_last_col

    csv_out.writerow(header)

    list_values = []
    # recorremos las filas de cada banco
    for row_number in range(13, end_fil):
        list_values = []  # reseteamos los valores
        # recorremos las columnas para eliminar espacios en blanco entre ellas
        for col in wsh.row_values(row_number, 0, last_col):
            if(col != ''):
                if(col == 'Banco de Brasil S.A.'):
                    col = 'Banco do Brasil S.A.'

                list_values.append(col)

        csv_out.writerow(list_values)
    fh.close()
    print("CSV file created successfully: "+fileName)


def pivot_csv(out_csv):

    contenido_csv = os.listdir(out_csv)

    for file_csv in contenido_csv:
        # leer csv como dataframe
        df_csv = pd.read_csv(out_csv+file_csv, sep=';')
        # hacer el pivoteo de los datos
        df_unpivoted = df_csv.melt(
            id_vars=['Instituciones'], var_name='Producto', value_name='Monto')

        df_unpivoted.to_csv(out_csv+file_csv, sep=';', index=False)
        print("Pivot file created successfully: "+file_csv)


# Ruta de entrada
location_excel = '/root/poc_cmf/data/'
# Leer Archivos csv para realizar el pivot
out_csv = '/root/poc_cmf/csv_utf_python/'  # Ruta de Salida
contenido = os.listdir(location_excel)

print contenido
print '\n'
for files in contenido:
    excel_to_csv_activos_1(files, location_excel, out_csv)
    excel_to_csv_activos_2(files, location_excel, out_csv)
    print '\n'

pivot_csv(out_csv)
