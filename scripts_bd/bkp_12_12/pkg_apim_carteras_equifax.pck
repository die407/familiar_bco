CREATE OR REPLACE PACKAGE BFAPIM.pkg_apim_carteras_equifax
IS
 v_scripts_directory_rechazados constant varchar2(100) := 'ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$SALIDA$EQUIFAX';
 v_nombre_ejecutable   constant varchar2(100) := 'get_files_1.sh';

 /*PROCEDURE pr_apim_cartera_alta;
 
 PROCEDURE pr_apim_cartera_baja;*/
 
 PROCEDURE pr_apim_cartera_rechazados;
 
 PROCEDURE pr_swf_crea_get_file_sh;
 
 PROCEDURE pr_mover_archivo(p_archivo_origen IN VARCHAR2, p_destino VARCHAR2);
 
 PROCEDURE pr_crea_script_sh(p_config_key IN VARCHAR2, p_directorio_destino IN VARCHAR2, p_archivo_nombre IN VARCHAR2);
 
 PROCEDURE pr_apim_novedades;
 
 PROCEDURE pr_apim_alta_carteras_fisicas;
 
 PROCEDURE pr_apim_insert_cliente_antecedente(p_filas_insertadas OUT NUMBER);
 
 PROCEDURE pr_apim_baja_carteras_fisicas;
 
 --PROCEDURE pr_apim_insert_apim_archivos_equifax(p_nombre IN VARCHAR2, p_fecha IN DATE, p_estado IN VARCHAR2, p_tipo IN VARCHAR2);
 
 FUNCTION pr_apim_2array2value (p_line VARCHAR2, p_position IN NUMBER) RETURN VARCHAR2;
 
 PROCEDURE pr_apim_insert_cliente_verificar(p_cod_cliente IN VARCHAR2, p_nro_doc_id IN VARCHAR2, 
                                            p_cod_tipo_doc_id IN NUMBER, p_nombre IN VARCHAR2, 
                                            p_apellido IN VARCHAR2, p_tipo_control IN VARCHAR2,
                                            p_tipo_movimiento IN VARCHAR2, p_cod_estado IN VARCHAR2,
                                            p_fec_actualizacion IN DATE, p_comentario IN VARCHAR2);
                                            
 PROCEDURE pr_apim_cartera_confirmados;
 
 PROCEDURE pr_apim_insert_clientes_controlados(nro_doc_id  IN VARCHAR2, nombre1  IN VARCHAR2, 
                                               nombre2  IN VARCHAR2, apellido IN VARCHAR2,
                                               apellido2 IN VARCHAR2, apellido_casada IN VARCHAR2,
                                               tipo_control IN VARCHAR2, fecha_inicio IN DATE, 
                                               fecha_final IN DATE,  cliente_controlado IN VARCHAR2, 
                                               cod_cliente IN VARCHAR2);
                                               
  PROCEDURE pr_apim_insert_apim_equifax_log(p_hora_evento IN date, p_sqlcode IN VARCHAR2,
                                            p_sqlerrm IN VARCHAR2, p_mensaje IN varchar2, 
                                            p_from_objecto IN varchar2, p_data IN CLOB);
                                            
  PROCEDURE pr_apim_ejecuta_confirm_rechazado;
END;
/
CREATE OR REPLACE PACKAGE BODY BFAPIM.pkg_apim_carteras_equifax
IS

 /*PROCEDURE pr_apim_cartera_alta
   IS
     archivo UTL_FILE.FILE_TYPE;
     linea VARCHAR2(32767);
     v_json_object_idbanco CLOB;
     v_id_banco_equifax VARCHAR2(200);
     v_nombre_archivo VARCHAR2(200);
     
     CURSOR c_datos IS
      select  pf.cod_cliente, 
        cl.nro_doc_id,    
        cl.cod_tipo_doc_id,     
        coalesce(pf.nombre1, cl.nombre) nombre1,  
        TRIM(pf.nombre2) nombre2,  
        coalesce(pf.apellido, ' ') apellido, 
        coalesce(pf.apellido2, ' ') apellido2,  
        coalesce(pf.apellido_casada,' ') apellido_casada, 
        pf.sexo,
        'AL' 
     from 
         ingres.persona_fisica pf
       join ingres.cliente cl on pf.cod_cliente=cl.cod_cliente 
       join ingres.cliente_estadistica ce on pf.cod_cliente = ce.cod_cliente 
       join ingres.supervisor_oficial so on cl.cod_supervisor = so.cod_supervisor  
            and cl.cod_oficial = so.cod_oficial 
       join ingres.resumen_mora_diario  rm on pf.cod_cliente = rm.cod_cliente 
       join ingres.operacion op  on rm.cod_sucursal  = op.cod_sucursal 
            and rm.nro_operacion = op.nro_operacion 
       join ingres.cliente_operacion co on co.cod_sucursal= op.cod_sucursal 
            and co.nro_operacion= op.nro_operacion 
       join ingres.prestamo  pr on rm.cod_sucursal  = pr.cod_sucursal 
            and rm.nro_operacion = pr.nro_operacion 
            and ce.ult_cod_sucursal = pr.cod_sucursal 
            and ce.ult_nro_operacion = pr.nro_operacion 
       where cl.cod_tipo_doc_id=1
         and rm.venta_cartera=1
         and ((rm.cod_producto in (1,10) and rm.area in ('FA','OI')) or (rm.cod_producto in (22) 
         and rm.area in ('MI')))
         and so.cod_tipo_oficial in (0,1,2,13)
         and pr.ult_cuo_cobrada >= 4
         and ce.cod_calificacion not in (40,41,42,50,51,52)
         and cl.cod_estado_cliente != 'BL'
         and op.cod_destino_oper not in (42,46,49) 
         and co.rol_cliente='CP' 
         and not exists (select 1 from ingres.sih_clientes_controlados cc where pf.cod_cliente=cc.cod_cliente) 
       
      UNION 

      select pf.cod_cliente, 
           cl.nro_doc_id,   
           cl.cod_tipo_doc_id,  
           coalesce(pf.nombre1, cl.nombre) nombre1,
           TRIM(pf.nombre2) nombre2,
           coalesce(pf.apellido, ' ') apellido,
           coalesce(pf.apellido2, ' ') apellido2,
           coalesce(pf.apellido_casada,' ') apellido_casada,
           pf.sexo,
           'AL' 
      from ingres.persona_fisica  pf
           join ingres.cliente  cl on pf.cod_cliente=cl.cod_cliente 
           join ingres.cliente_estadistica ce on pf.cod_cliente = ce.cod_cliente 
           join ingres.supervisor_oficial so on cl.cod_supervisor =so.cod_supervisor 
                and cl.cod_oficial = so.cod_oficial 
           join ingres.operacion op  on ce.ult_cod_sucursal  = op.cod_sucursal 
                and ce.ult_nro_operacion = op.nro_operacion 
           join ingres.cliente_operacion co on co.cod_sucursal= op.cod_sucursal 
                and co.nro_operacion= op.nro_operacion 
        where cl.cod_tipo_doc_id=1 
            and so.cod_tipo_oficial in (0,1,2,13)
            and ce.cant_prestamos_act=0
            AND ce.cant_prestamos_canc > 0
            AND ((ce.fec_ult_canc > add_months(to_date(sysdate), -12) AND cod_producto=1)OR
                (ce.fec_ult_canc > add_months(to_date(sysdate), -6) AND cod_producto=10)OR
                (ce.fec_ult_canc > add_months(to_date(sysdate), -12) AND cod_producto=22 AND cl.area='MI'))
            AND ce.cod_calificacion not in (40,41,42,50,51,52)
            AND cl.cod_estado_cliente != 'BL'
            AND op.cod_destino_oper not in (42,46,49)
            AND op.cod_empresa != 3 
            AND op.cod_estado_operacion='CA' 
            AND co.rol_cliente='CP' 
            AND NOT EXISTS (SELECT 1 FROM ingres.sih_clientes_controlados cc where pf.cod_cliente=cc.cod_cliente);
   
   BEGIN
     BEGIN
       SELECT t.config_data INTO v_json_object_idbanco 
       FROM bfapim.apim_config t
       WHERE t.config_key = 'id_banco_equifax';
       v_id_banco_equifax := JSON_VALUE(v_json_object_idbanco, '$.id_banco');
     EXCEPTION
       WHEN OTHERS THEN
         raise_application_error(-20001, 'Error al obtener id_banco para equifax');
     END;
     v_nombre_archivo := 'PY_'||v_id_banco_equifax||'_Altas_'||to_char(SYSDATE, 'ddmmyyyy');
     archivo := UTL_FILE.FOPEN('ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX', v_nombre_archivo||'.csv', 'W');
     --archivo := UTL_FILE.FOPEN('ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$SALIDA$EQUIFAX', v_nombre_archivo||'.csv', 'W');
     linea := 'Documento,Tipo Documento,Nombre/Raz�n Social,Sexo,Cartera,Calle,Altura,Piso y Dpto,Localidad,C�digo Postal,Provincia,Tel�fono,Operaci�n,Tiempo de Control,Ejecutivo,Sucursal,Tipo Cliente';
     UTL_FILE.PUT_LINE(archivo, linea);
     
     FOR fila IN c_datos LOOP
      linea := fila.nro_doc_id || ',' || 'CI' || ',' || fila.nombre1 || CASE WHEN fila.nombre2 IS NOT NULL  
                                                                        THEN ' '||fila.nombre2
                                                                        ELSE ''
                                                                        END ||
               ' '||fila.apellido || ',' || fila.sexo ||',,,,,,,,,A,,,,' ;
      UTL_FILE.PUT_LINE(archivo, linea);
      \*BEGIN
        INSERT INTO ingres.sih_clientes_controlados(nro_doc_id, nombre1, 
                                                    nombre2, apellido, 
                                                    apellido2, apellido_casada, 
                                                    tipo_control, fecha_inicio, 
                                                    fecha_final, cliente_controlado, 
                                                    cod_cliente)
                                             VALUES (fila.nro_doc_id, fila.nombre1,
                                                     fila.nombre2, fila.apellido,
                                                     fila.apellido2, fila.apellido_casada,
                                                     'EFECTIVO', SYSDATE,
                                                     NULL, 'SI',
                                                     FILA.COD_CLIENTE);
      EXCEPTION
        WHEN OTHERS THEN
          raise_application_error(-20001, 'Error al insertar en ingres.sih_clientes_controlados. '||SQLERRM);
      END;*\
     END LOOP;

     -- Cerrar el archivo
     UTL_FILE.FCLOSE(archivo);
     --COMMIT;
     EXCEPTION
       -- Manejo de excepciones para posibles errores
       WHEN UTL_FILE.INVALID_PATH THEN
          DBMS_OUTPUT.PUT_LINE('Error: Ruta de archivo inv�lida.');
       WHEN UTL_FILE.WRITE_ERROR THEN
          DBMS_OUTPUT.PUT_LINE('Error: No se puede escribir en el archivo.');
       WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
           --Cerrar el archivo si ocurre un error
           IF UTL_FILE.IS_OPEN(archivo) THEN
              UTL_FILE.FCLOSE(archivo);
           END IF;

   END;



   PROCEDURE pr_apim_cartera_baja
   IS
     archivo UTL_FILE.FILE_TYPE;
     linea VARCHAR2(32767);
     v_json_object_idbanco CLOB;
     v_id_banco_equifax VARCHAR2(200);
     v_nombre_archivo VARCHAR2(200);
     
     CURSOR c_datos IS
      SELECT pf.cod_cliente,
             cl.nro_doc_id,
             cl.cod_tipo_doc_id,
             coalesce(pf.nombre1, cl.nombre) nombre1,
             TRIM(pf.nombre2) nombre2,
             coalesce(pf.apellido, ' ') apellido,
             coalesce(pf.apellido2, ' '),
             coalesce(pf.apellido_casada, ' '),
             pf.sexo,
             'BA'
        FROM ingres.persona_fisica           pf,
             ingres.cliente                  cl,
             ingres.cliente_estadistica      ce,
             ingres.sih_clientes_controlados cc,
             ingres.resumen_mora_diario      rm,
             ingres.operacion                op,
             ingres.prestamo                 pr,
             ingres.supervisor_oficial       so
       WHERE pf.cod_cliente = cl.cod_cliente
         AND pf.cod_cliente = ce.cod_cliente
         AND pf.cod_cliente = cc.cod_cliente
         AND pf.cod_cliente = rm.cod_cliente
         AND rm.cod_sucursal = op.cod_sucursal
         AND rm.nro_operacion = op.nro_operacion
         AND rm.cod_sucursal = pr.cod_sucursal
         AND rm.nro_operacion = pr.nro_operacion
         AND ce.ult_cod_sucursal = op.cod_sucursal
         AND ce.ult_nro_operacion = op.nro_operacion
         AND cl.cod_supervisor = so.cod_supervisor
         AND cl.cod_oficial = so.cod_oficial
         AND cl.cod_tipo_doc_id = 1
         AND rm.venta_cartera = 1
         AND so.cod_tipo_oficial IN (0, 1, 2, 13)
         AND ((rm.cod_producto IN (1, 10) AND rm.area IN ('FA', 'OI')) OR
             (rm.cod_producto IN (22) AND rm.area IN ('MI')))
         AND (ce.cod_calificacion IN (40, 41, 42, 50, 51, 52) OR
             cl.cod_estado_cliente = 'BL' OR pr.ult_cuo_cobrada < 4 OR
             op.cod_destino_oper IN (42, 46, 49) OR op.cod_empresa = 3)
     UNION    
      SELECT pf.cod_cliente,
             cl.nro_doc_id,
             cl.cod_tipo_doc_id,
             coalesce(pf.nombre1, cl.nombre) nombre1,
             TRIM(pf.nombre2) nombre2,
             coalesce(pf.apellido, ' ') apellido,
             coalesce(pf.apellido2, ' '),
             coalesce(pf.apellido_casada, ' '),
             pf.sexo,
             'BA'
        FROM ingres.persona_fisica           pf,
             ingres.cliente                  cl,
             ingres.cliente_estadistica      ce,
             ingres.sih_clientes_controlados cc,
             ingres.operacion                op
       WHERE pf.cod_cliente = cl.cod_cliente
         AND pf.cod_cliente = ce.cod_cliente
         AND pf.cod_cliente = cc.cod_cliente
         AND ce.ult_cod_sucursal = op.cod_sucursal
         AND ce.ult_nro_operacion = op.nro_operacion
         AND cl.cod_tipo_doc_id = 1
         AND ce.cant_prestamos_act = 0
         AND ce.cant_prestamos_canc > 0
         AND ((ce.fec_ult_canc < add_months(to_date(SYSDATE), -12) AND
             cod_producto <> 10) OR
             ce.fec_ult_canc < add_months(to_date(SYSDATE), -6) AND
             cod_producto IN (10))
         AND (ce.cod_calificacion IN (40, 41, 42, 50, 51, 52) OR
             cl.cod_estado_cliente = 'BL' OR
             op.cod_destino_oper IN (42, 46, 49) OR op.cod_empresa = 3);
   
   BEGIN
     BEGIN
       SELECT t.config_data INTO v_json_object_idbanco 
       FROM bfapim.apim_config t
       WHERE t.config_key = 'id_banco_equifax';
       v_id_banco_equifax := JSON_VALUE(v_json_object_idbanco, '$.id_banco');
     EXCEPTION
       WHEN OTHERS THEN
         raise_application_error(-20001, 'Error al obtener id_banco para equifax');
     END;
     v_nombre_archivo := 'PY_'||v_id_banco_equifax||'_Bajas_'||to_char(SYSDATE, 'ddmmyyyy');
     archivo := UTL_FILE.FOPEN('ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX', v_nombre_archivo||'.csv', 'W');
     linea := 'Documento,Tipo Documento,Nombre/Raz�n Social,Sexo,Cartera,Calle,Altura,Piso y Dpto,Localidad,C�digo Postal,Provincia,Tel�fono,Operaci�n,Tiempo de Control,Ejecutivo,Sucursal,Tipo Cliente';
     UTL_FILE.PUT_LINE(archivo, linea);
     
     FOR fila IN c_datos LOOP
      linea := fila.nro_doc_id || ',' || 'CI' || ',' || fila.nombre1 || CASE WHEN fila.nombre2 IS NOT NULL  
                                                                        THEN ' '||fila.nombre2
                                                                        ELSE ''
                                                                        END ||
               ' '||fila.apellido || ',' || fila.sexo ||',,,,,,,,,A,,,,' ;
      UTL_FILE.PUT_LINE(archivo, linea);
     END LOOP;

     -- Cerrar el archivo
     UTL_FILE.FCLOSE(archivo);
     
     EXCEPTION
       -- Manejo de excepciones para posibles errores
       WHEN UTL_FILE.INVALID_PATH THEN
          DBMS_OUTPUT.PUT_LINE('Error: Ruta de archivo inv�lida.');
       WHEN UTL_FILE.WRITE_ERROR THEN
          DBMS_OUTPUT.PUT_LINE('Error: No se puede escribir en el archivo.');
       WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
           --Cerrar el archivo si ocurre un error
           IF UTL_FILE.IS_OPEN(archivo) THEN
              UTL_FILE.FCLOSE(archivo);
           END IF;

   END;*/
   
   PROCEDURE pr_apim_cartera_rechazados
   IS
     file_handle UTL_FILE.FILE_TYPE;
     file_line       VARCHAR2(32767);
     archivo_entrada UTL_FILE.FILE_TYPE;
     linea           varchar2(32767);
     --v_cod_cliente   NUMBER;
     v_nro_doc    VARCHAR2(100);
     v_comentario VARCHAR2(200);
     v_tipo_doc_str VARCHAR2(20);
     v_tipo_doc_num NUMBER;
     v_cod_cliente VARCHAR2(11);
     v_nombre     VARCHAR2(65);
     v_apellido   VARCHAR2(20);
     v_tipo_control char(8);
     v_tipo_movimiento char(2);
     v_cod_estado char(2);
     v_fec_actualizacion DATE;
     v_rowcount NUMBER := 0;
     
     v_file_name     VARCHAR2(255);
     v_fecha_nombre_archivo VARCHAR2(20);
     v_dioError BOOLEAN;
     v_se_inserto BOOLEAN := FALSE;
   BEGIN
    DBMS_OUTPUT.ENABLE(1000000);
    -- Creacion del .sh que genera el txt con el listado de archivos csv
    -- Se comenta la creacion del .sh porque va quedar fijo en el servidor con los permisos adecuados
    --pr_swf_crea_get_file_sh;
    dbms_output.put_line('Inicio proceso RECHAZADOS');
    --ejecuta el archivo .sh que crea el archivo output_mx.txt y output_mt.txt que contiene el listado de archivos en el directorio
    BEGIN
      /*BEGIN
        dbms_output.put_line('creacion de job');
          DBMS_SCHEDULER.CREATE_JOB (
              job_name        => 'BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS',
              job_type        => 'EXECUTABLE',
              --job_action      => '/bin/ls',  -- Cambia esto seg�n tu sistema operativo
              job_action      => '/archivos_aplicacion/salida/otras_instituciones/equifax/get_files_1.sh',
              number_of_arguments => 0,
              auto_drop       => TRUE,
              enabled         => TRUE
                  );
      EXCEPTION
        WHEN OTHERS THEN
          dbms_output.put_line('Error en create_job. '||SQLERRM);
      END;
      dbms_output.put_line('TERMINADO CREACION JOB');*/
      /*dbms_scheduler.set_job_argument_value(job_name          => 'BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS',
                                            argument_position => 1 ,
                                            argument_value    => '/archivos_aplicacion/salida/otras_instituciones/equifax/get_files_1.sh');*/
      BEGIN
        DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS');
        DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS');
      EXCEPTION
        WHEN OTHERS THEN
          pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al ejecutar Job LIST_FILES_EQUIFAX_RECHAZOS. '||SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                            'pr_apim_cartera_rechazados', NULL);
          bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al ejecutar Job LIST_FILES_EQUIFAX_RECHAZOS, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
          raise_application_error(-20015, 'Error en JOB. '||SQLERRM);
      END;
      dbms_output.put_line('TERMINADO RUN JOB para listar archivos csv');
      -- Abrir el archivo de salida (donde se encuentra la lista de archivos obtenidos)
      archivo_entrada := utl_file.fopen(v_scripts_directory_rechazados, 'output_mt.txt', 'R');
      
      LOOP
        v_dioError := FALSE;
        v_se_inserto := FALSE;
        BEGIN
          utl_file.get_line(archivo_entrada, linea);
          --AQUI PROCESAR EL ARCHIVO TXT OBTENIDO (se obtiene un archivo ej: listado.txt)
          v_file_name := linea; --para tener ordenado el codigo se guarda en la variable

          IF v_file_name LIKE '%Rechazos%' THEN
            v_fecha_nombre_archivo := SUBSTR(v_file_name, instr(v_file_name, '_Altas_') + 7, 8);
            dbms_output.put_line('Procesando: '|| v_file_name || ' Fecha: '||v_fecha_nombre_archivo);
            /*Tratamiento del o de los archivos csv de rechazados*/
            file_handle := UTL_FILE.FOPEN(v_scripts_directory_rechazados, v_file_name, 'R');

            LOOP
              BEGIN
                  -- Leer una l�nea del archivo
                  UTL_FILE.GET_LINE(file_handle, file_line);
                  v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 2);
                  
                  IF v_nro_doc != 'Documento' THEN
                    -- Separar los valores del CSV (usualmente separados por , o ;)
                    BEGIN
                      v_comentario := pr_apim_2array2value (file_line, 18);
                      v_tipo_doc_str := pr_apim_2array2value (file_line, 3);
                      DBMS_OUTPUT.PUT_LINE( 'Documento a procesar:' || v_nro_doc||' - v_comentario: '|| v_comentario);
                    EXCEPTION
                      WHEN OTHERS THEN
                        v_dioError := TRUE;
                        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al extraer datos de una linea del archivo csv. ' || SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                            'pr_apim_cartera_rechazados', NULL);
                        bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al extraer datos de una linea del archivo csv, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                        DBMS_OUTPUT.PUT_LINE('Error al obtener datos de la linea. '||SQLERRM);
                    END;
                    -- OBTENER EQUIVALENCIA DEL TIPO DE DOCUMENTO
                    /*IF v_tipo_doc_str = 'CI' THEN
                      v_tipo_doc_num := 1;
                    ELSIF v_tipo_doc_str = 'RUC' THEN
                      v_tipo_doc_num := 4;
                    END IF;*/
                    BEGIN
                      SELECT cod_tipo_doc_id INTO v_tipo_doc_num
                      from ingres.tipo_documento_id
                      WHERE TRIM(abrev_tipo_doc_id) = TRIM(v_tipo_doc_str);
                    EXCEPTION
                      WHEN OTHERS THEN
                        v_dioError := TRUE;
                        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al consultar ingres.tipo_documento_id, '|| SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                            'pr_apim_cartera_rechazados', NULL);
                        bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al consultar ingres.tipo_documento_id, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                        DBMS_OUTPUT.PUT_LINE('Error al recuperar tipo documento. '||SQLERRM);
                    END;

                    -- ACTUALIZAR CLIENTE_ANTECEDENTE
                    -- tomar el registro mas reciente para actualizar max(fec_actualizacion)
                    IF v_comentario != 'Error de validacion: Ya esta en seguimiento' 
                       AND v_dioError = FALSE THEN
                      BEGIN
                          UPDATE ingres.sih_cliente_antecedente
                          SET cod_estado = 'ER',
                              comentario = v_comentario,
                              fec_actualizacion = SYSDATE
                          WHERE nro_doc_id = v_nro_doc
                            AND cod_tipo_doc_id = v_tipo_doc_num
                            AND fec_actualizacion = (SELECT MAX(fec_actualizacion) 
                                                    FROM ingres.sih_cliente_antecedente
                                                    WHERE nro_doc_id = v_nro_doc
                                                      AND cod_tipo_doc_id = v_tipo_doc_num); --to_date(v_fecha_nombre_archivo, 'DDMMYYYY');
                            dbms_output.put_line('cliente_antecedente actualizado '||v_nro_doc); 
                      EXCEPTION
                        WHEN OTHERS THEN
                          v_dioError := TRUE;
                          pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                              SQLERRM, 'Error al actualizar sih_cliente_antecedente. '|| SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                              'pr_apim_cartera_rechazados', NULL);
                          bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al actualizar sih_cliente_antecedente, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                                              
                          dbms_output.put_line('Error al actualizar en ingres.sih_cliente_antecedente. '||SQLERRM);
                      END;
                    
                      v_rowcount := v_rowcount + SQL%ROWCOUNT;
                    
                      -- OBTENER CODIGO DE CLIENTE Y OTROS DATOS
                      IF v_dioError = FALSE THEN
                        BEGIN
                          SELECT cod_cliente, primer_nombre, primer_apellido,
                                 tipo_control, tipo_movimiento, cod_estado 
                                 --,fec_actualizacion
                          INTO v_cod_cliente, v_nombre, v_apellido,
                               v_tipo_control, v_tipo_movimiento, v_cod_estado
                               --,v_fec_actualizacion
                          FROM ingres.sih_cliente_antecedente
                          WHERE nro_doc_id = v_nro_doc
                            AND cod_tipo_doc_id = v_tipo_doc_num
                            AND trunc(fec_actualizacion) = trunc(SYSDATE)
                            AND ROWNUM = 1;
                          v_fec_actualizacion := SYSDATE;
                        EXCEPTION
                          WHEN OTHERS THEN
                          v_dioError := TRUE;
                          pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                                SQLERRM, 'Error al obtener datos de sih_cliente_antecedente. ' || SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                                'pr_apim_cartera_rechazados', NULL);
                          bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al obtener datos de sih_cliente_antecedente, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                                                
                          dbms_output.put_line('Error al recuperar ingres.sih_cliente_antecedente. '||SQLERRM);
                        END;
                      END IF;
                      
                      IF v_dioError = FALSE THEN
                        BEGIN
                          pr_apim_insert_cliente_verificar(v_cod_cliente,       v_nro_doc, 
                                                           v_tipo_doc_num,      v_nombre, 
                                                           v_apellido,          v_tipo_control, 
                                                           v_tipo_movimiento,   v_cod_estado, 
                                                           v_fec_actualizacion, v_comentario);
                          v_se_inserto := TRUE; -- Solo si llego a este punto se considera que inserto al menos uno
                        EXCEPTION
                          WHEN DUP_VAL_ON_INDEX THEN
                            UPDATE ingres.sih_cliente_verificar
                            SET fec_actualizacion = v_fec_actualizacion,
                                comentario = v_comentario
                            WHERE cod_cliente = v_cod_cliente;
                          WHEN OTHERS THEN
                            v_dioError := TRUE;
                            pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                                  SQLERRM,'Error al insertar datos en sih_cliente_verificar. ' || SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                                  'pr_apim_cartera_rechazados', NULL);
                            bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al insertar datos en sih_cliente_verificar, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                                                  
                            dbms_output.put_line('Error al insertar cliente_verificar. '||SQLERRM);
                        END;
                      END IF;
                    END IF;
                  END IF;
              EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                      -- Finalizar el bucle cuando no haya m�s datos en el archivo
                      EXIT;
              END;
            END LOOP;
            UTL_FILE.FCLOSE(file_handle); --cierra el csv de rechazados
            
            --IF v_rowcount > 0 THEN
            IF v_se_inserto = TRUE THEN
              COMMIT;
            END IF;
            -- Correr script bash que mueve el archivo procesado
            DBMS_OUTPUT.PUT_LINE( 'Moviendo documento '|| v_file_name);
            pr_mover_archivo('/archivos_aplicacion/salida/otras_instituciones/equifax/'||v_file_name, '/archivos_aplicacion/salida/otras_instituciones/equifax/procesados/');
            DBMS_OUTPUT.PUT_LINE( 'Documento movido a: ' || '/archivos_aplicacion/salida/otras_instituciones/equifax/procesados/');
            
            -- AQUI LLAMAR AL PROCEDIMIENTO DE CONFIRMADOS (probablemente se crea otro procedimiento que llame a RECHAZADOS y CONFIRMADOS)
            
            
          END IF;
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya m�s datos en el archivo
                  EXIT;
        END;
      END LOOP;
      UTL_FILE.FCLOSE(archivo_entrada);
    END;
   END;
   
  procedure pr_swf_crea_get_file_sh
   is
    file_handle UTL_FILE.FILE_TYPE;
    v_contenido_json clob;
    v_script VARCHAR2(4000);
  BEGIN
    SELECT config_data
      INTO v_contenido_json
      FROM bfapim.apim_config
     WHERE config_key = 'script_listar_rechazados';
     v_script := JSON_VALUE(v_contenido_json, '$.script');
     
      file_handle := UTL_FILE.FOPEN(v_scripts_directory_rechazados, v_nombre_ejecutable, 'W');

      UTL_FILE.PUT_LINE(file_handle, v_script);

      UTL_FILE.FCLOSE(file_handle);
  EXCEPTION
      WHEN OTHERS THEN
          IF UTL_FILE.IS_OPEN(file_handle) THEN
              UTL_FILE.FCLOSE(file_handle);
--              UTL_FILE.FREMOVE(v_archivo_dir_output,v_nombre_ejecutable);
          END IF;
          RAISE;
 END pr_swf_crea_get_file_sh;


  PROCEDURE pr_mover_archivo(p_archivo_origen IN VARCHAR2, p_destino VARCHAR2)
    IS
    
    BEGIN
      -- Crear el mover_archivo_rec_proc.sh en 
      -- Se comenta porque el .sh se va crear por unica vez en el server con los permisos necesarios
      --pr_crea_script_sh('script_mv_rec_proc', 'ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$SALIDA$EQUIFAX', 'mover_archivo_rec_proc.sh');
      
      -- Crear el Job, ver si esto va quedar aca o se va crear independientemente con autodrop FALSE
      -- Se comenta la creacion del job porque va quedar fijo en la bd
      /*DBMS_SCHEDULER.CREATE_JOB(
          job_name        => 'BFAPIM.JOB_MOVER_ARCHIVO_REC_PROC',
          job_type        => 'EXECUTABLE',
          job_action      => '/archivos_aplicacion/salida/otras_instituciones/equifax/mover_archivo_rec_proc.sh', -- Ruta completa al script Bash
          number_of_arguments => 2,                      -- N�mero de argumentos
          auto_drop       => TRUE,                      -- Mantener el Job despu�s de ejecutarse
          enabled         => FALSE                       -- No lo habilitamos a�n
      );*/

      -- Definir los par�metros
      DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(
          job_name        => 'BFAPIM.JOB_MOVER_ARCHIVO_REC_PROC',
          argument_position => 1,  -- Primer argumento: archivo
          argument_value  => p_archivo_origen
      );

      DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(
          job_name        => 'BFAPIM.JOB_MOVER_ARCHIVO_REC_PROC',
          argument_position => 2,  -- Segundo argumento: directorio destino
          argument_value  => p_destino
      );

      -- Habilitar el Job
      DBMS_SCHEDULER.ENABLE('BFAPIM.JOB_MOVER_ARCHIVO_REC_PROC');

      -- Opcional: Ejecutar el Job inmediatamente
      DBMS_SCHEDULER.RUN_JOB('BFAPIM.JOB_MOVER_ARCHIVO_REC_PROC');
    END;
    
    PROCEDURE pr_crea_script_sh(p_config_key IN VARCHAR2, p_directorio_destino IN VARCHAR2, p_archivo_nombre IN VARCHAR2)
      IS
        file_handle UTL_FILE.FILE_TYPE;
        v_contenido_json clob;
        v_script VARCHAR2(4000);
      BEGIN
        SELECT config_data
          INTO v_contenido_json
          FROM bfapim.apim_config
         WHERE config_key = p_config_key;
         v_script := JSON_VALUE(v_contenido_json, '$.script');
         
         file_handle := UTL_FILE.FOPEN(p_directorio_destino, p_archivo_nombre, 'W');
         UTL_FILE.PUT_LINE(file_handle, v_script);
         UTL_FILE.FCLOSE(file_handle);
      EXCEPTION
          WHEN OTHERS THEN
              IF UTL_FILE.IS_OPEN(file_handle) THEN
                  UTL_FILE.FCLOSE(file_handle);
              END IF;
              RAISE;
      END;
      
 PROCEDURE pr_apim_novedades
   AS
     file_handle UTL_FILE.FILE_TYPE;
     file_line       VARCHAR2(32767);
     archivo_entrada UTL_FILE.FILE_TYPE;
     linea           varchar2(32767);
     v_nro_doc    VARCHAR2(100);
     v_file_name     VARCHAR2(255);
   BEGIN
     /*PROCEDIMIENTO PARA PROCESO DE ARCHIVOS DE NOVEDADES*/
     -- Listar 
     DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_NOVEDADES');
     DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_NOVEDADES');
     
     archivo_entrada := utl_file.fopen(v_scripts_directory_rechazados, 'output_mt.txt', 'R');
      
      LOOP
        BEGIN
          utl_file.get_line(archivo_entrada, linea);
          --AQUI PROCESAR EL ARCHIVO TXT OBTENIDO (se obtiene un archivo ej: listado.txt)
          v_file_name := linea; --para tener ordenado el codigo

          IF v_file_name LIKE '%Novedades%' THEN
   dbms_output.put_line('Procesando: '|| v_file_name);
            /*Tratamiento del o de los archivos csv de */
            file_handle := UTL_FILE.FOPEN('ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX$NOVEDADES', v_file_name, 'R');
            LOOP
              BEGIN
                  -- Leer una l�nea del archivo
                  UTL_FILE.GET_LINE(file_handle, file_line);
                  -- Separar los valores del CSV (usualmente separados por comas)
                  v_nro_doc := REGEXP_SUBSTR(file_line, '[^,]+', 1, 1);
                  /*v_cod_cliente := REGEXP_SUBSTR(file_line, '[^,]+', 1, 2);
                  v_nombre := REGEXP_SUBSTR(file_line, '[^,]+', 1, 3);*/
                  IF v_nro_doc != 'Documento' THEN
                    DBMS_OUTPUT.PUT_LINE( 'Documento a procesar:' || v_nro_doc);                 
                    --AQUI PROCESAR EL NRO DE DOCUMENTO
                    -- Ej.: update xxxx set estado = 'R' where nro_doc = v_nro_doc;
                  END IF;
              EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                      -- Finalizar el bucle cuando no haya m�s datos en el archivo
                      EXIT;
              END;
            END LOOP;
            UTL_FILE.FCLOSE(file_handle); --cierra el csv de novedades
            -- Correr script bash que mueve el archivo procesado
            DBMS_OUTPUT.PUT_LINE( 'Moviendo documento '|| v_file_name);
            pr_mover_archivo('/archivos_aplicacion/salida/otras_instituciones/equifax/novedades'||v_file_name, '/archivos_aplicacion/salida/otras_instituciones/equifax/novedades/procesados/');
            DBMS_OUTPUT.PUT_LINE( 'Documento movido a: ' || '/archivos_aplicacion/salida/otras_instituciones/equifax/novedades/procesados/');
          END IF;
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya m�s datos en el archivo
                  EXIT;
        END;
      END LOOP;
      UTL_FILE.FCLOSE(archivo_entrada);
     
   END;
   
   PROCEDURE pr_apim_alta_carteras_fisicas
   AS
     archivo UTL_FILE.FILE_TYPE;
     linea VARCHAR2(32767);
     v_json_object_idbanco CLOB;
     v_id_banco_equifax VARCHAR2(200);
     v_nombre_archivo VARCHAR2(200);
     v_filas_insertadas NUMBER := 0;
     v_dioError BOOLEAN;
     
   BEGIN
     --Insercion en tabla temporal
     BEGIN 
      DELETE bfapim.temp_carteras_table;
      INSERT INTO bfapim.temp_carteras_table(cod_cliente,     nro_doc_id, 
                                                  cod_tipo_doc_id, nombre1, 
                                                  nombre2,         apellido, 
                                                  apellido2,       apellido_casada, 
                                                  sexo,       operacion)
      select  pf.cod_cliente,       
           cl.nro_doc_id,           
           cl.cod_tipo_doc_id,          
           coalesce(pf.nombre1, cl.nombre),     
           coalesce(pf.nombre2, ' '),       
           coalesce(pf.apellido, ' '),        
           coalesce(pf.apellido2, ' '),       
           coalesce(pf.apellido_casada,' '),
           substr(TRIM(pf.sexo),1,1),
           'AL'             
          from     
          ingres.persona_fisica  pf     
          join ingres.cliente  cl on pf.cod_cliente=cl.cod_cliente     
          join ingres.cliente_estadistica ce on pf.cod_cliente = ce.cod_cliente     
          join ingres.supervisor_oficial so on cl.cod_supervisor = so.cod_supervisor  and cl.cod_oficial = so.cod_oficial     
          join ingres.resumen_mora_diario  rm on pf.cod_cliente = rm.cod_cliente     
          join ingres.operacion op  on rm.cod_sucursal  = op.cod_sucursal and rm.nro_operacion = op.nro_operacion     
          join ingres.cliente_operacion co on co.cod_sucursal= op.cod_sucursal and co.nro_operacion= op.nro_operacion    
          join ingres.prestamo  pr on rm.cod_sucursal  = pr.cod_sucursal and rm.nro_operacion = pr.nro_operacion and  
           ce.ult_cod_sucursal = pr.cod_sucursal and ce.ult_nro_operacion = pr.nro_operacion     
          where     
          cl.cod_tipo_doc_id=1    
          and rm.venta_cartera=1    
          and ((rm.cod_producto in (1,10) and rm.area in ('FA','OI')) or (rm.cod_producto in (22) and rm.area in ('MI')))    
          and so.cod_tipo_oficial in (0,1,2,13)    
          and pr.ult_cuo_cobrada >= 4    
          and ce.cod_calificacion not in (40,41,42,50,51,52)    
          and cl.cod_estado_cliente != 'BL'    
          and op.cod_destino_oper not in (42,46,49)     
          and co.rol_cliente='CP'    
          and not exists (select 1 from ingres.sih_clientes_controlados cc where pf.cod_cliente=cc.cod_cliente)     
        
          UNION     
        
      select pf.cod_cliente,    
         cl.nro_doc_id,           
         cl.cod_tipo_doc_id,          
         coalesce(pf.nombre1, cl.nombre),     
         coalesce(pf.nombre2, ' '),       
         coalesce(pf.apellido, ' '),        
         coalesce(pf.apellido2, ' '),       
         coalesce(pf.apellido_casada,' '),
         substr(TRIM(pf.sexo),1,1),  
         'AL'                  
          from     
          ingres.persona_fisica  pf    
          join ingres.cliente  cl on pf.cod_cliente=cl.cod_cliente     
          join ingres.cliente_estadistica ce on pf.cod_cliente = ce.cod_cliente     
          join ingres.supervisor_oficial so on cl.cod_supervisor =so.cod_supervisor and cl.cod_oficial = so.cod_oficial     
          join ingres.operacion op  on ce.ult_cod_sucursal  = op.cod_sucursal and ce.ult_nro_operacion = op.nro_operacion     
          join ingres.cliente_operacion co on co.cod_sucursal= op.cod_sucursal and co.nro_operacion= op.nro_operacion    
           where     
           cl.cod_tipo_doc_id=1    
           and so.cod_tipo_oficial in (0,1,2,13)    
           and ce.cant_prestamos_act=0    
           and ce.cant_prestamos_canc > 0    
           and ((ce.fec_ult_canc > add_months(to_date(sysdate), -12) and cod_producto=1)or    
                (ce.fec_ult_canc > add_months(to_date(sysdate), -6) and cod_producto=10)or    
                (ce.fec_ult_canc > add_months(to_date(sysdate), -12) and cod_producto=22 and cl.area='MI')    
               )    
           and ce.cod_calificacion not in (40,41,42,50,51,52)    
           and cl.cod_estado_cliente != 'BL'    
           and op.cod_destino_oper not in (42,46,49)    
           and op.cod_empresa != 3    
           and op.cod_estado_operacion='CA'    
           and co.rol_cliente='CP'    
           and not exists (select 1 from ingres.sih_clientes_controlados cc where pf.cod_cliente=cc.cod_cliente) ;
           
     EXCEPTION
       WHEN OTHERS THEN
         pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al insertar en tabla temporal los registros de alta de carteras fisicas. '||SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                            'pr_apim_alta_carteras_fisicas', NULL);
         bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al insertar en tabla temporal los registros de alta de carteras fisicas, en bfapim.pkg_apim_carteras_equifax.pr_apim_alta_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
         
         raise_application_error(-20031, 'Error en inserci�n '||SQLERRM);
     END;
      
     -- Insercion en ingres.sih_cliente_antecedente a partir de la tabla temporal
     BEGIN
       pr_apim_insert_cliente_antecedente(v_filas_insertadas);
     EXCEPTION
       WHEN OTHERS THEN
         pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al insertar en sih_cliente_antecedente. '||SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2), 
                                            'pr_apim_alta_carteras_fisicas', NULL);
         bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al insertar en sih_cliente_antecedente, en bfapim.pkg_apim_carteras_equifax.pr_apim_alta_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
         dbms_output.put_line('Error al insertar en sih_cliente_antecedente. '||SQLERRM);
     END;
     
     IF v_filas_insertadas = 0 THEN
       pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Query de altas no recupera ning�n registro', 
                                            'pr_apim_alta_carteras_fisicas', NULL);
       bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Query de altas no recupera ning�n registro, en bfapim.pkg_apim_carteras_equifax.pr_apim_alta_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
       dbms_output.put_line('Query de altas no recupera ning�n registro. '||SQLERRM);
     END IF;
     
     IF v_filas_insertadas > 0 THEN
        --Generar archivo csv
        BEGIN
           SELECT t.config_data INTO v_json_object_idbanco 
           FROM bfapim.apim_config t
           WHERE t.config_key = 'id_banco_equifax';
           v_id_banco_equifax := JSON_VALUE(v_json_object_idbanco, '$.id_banco');
        EXCEPTION
           WHEN OTHERS THEN
             pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al obtener id_banco para equifax', 
                                            'pr_apim_alta_carteras_fisicas', NULL);
             bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                    p_texto => 'Error al obtener id_banco para equifax, en bfapim.pkg_apim_carteras_equifax.pr_apim_alta_carteras_fisicas. '||SQLERRM,
                                    p_codigo_mail => 'MTEI');
             raise_application_error(-20180, 'Error al obtener id_banco para equifax');
        END;
        BEGIN
          v_nombre_archivo := 'PY_'||v_id_banco_equifax||'_Altas_'||to_char(SYSDATE, 'DDMMYYYYHH24MISS');
          archivo := UTL_FILE.FOPEN('ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX', v_nombre_archivo||'.csv', 'W');
          linea := 'Documento;Tipo Documento;Primer nombre;Segundo nombre;Primer apellido;Segundo apellido;Sexo;Cartera;Calle;Altura;Piso y Depto;Localidad;C�digo Postal;Provincia;Tel�fono;Operaci�n;Tiempo de Control;Ejecutivo;Sucursal;Tipo de Cliente';
          UTL_FILE.PUT_LINE(archivo, linea);
        FOR fila IN (SELECT x.cod_cliente, x.nro_doc_id, x.cod_tipo_doc_id, 
                         x.nombre1, TRIM(x.nombre2) nombre2, x.apellido, 
                         x.apellido2, x.apellido_casada, 
                         x.operacion, x.sexo FROM bfapim.temp_carteras_table x) LOOP
          linea := fila.nro_doc_id || ';' || 'CI' || ';' || fila.nombre1 || ';' || fila.nombre2 || ';' || fila.apellido || ';' || fila.apellido2 || ';' || fila.sexo ||';;;;;;;;;A;;;;' ;
          UTL_FILE.PUT_LINE(archivo, linea);
        END LOOP;
        UTL_FILE.FCLOSE(archivo);
        EXCEPTION
          WHEN OTHERS THEN
            pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al generar archivo csv para altas de cartera', 
                                            'pr_apim_alta_carteras_fisicas', NULL);
            bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                    p_texto => 'Error al generar archivo csv para altas de cartera, en bfapim.pkg_apim_carteras_equifax.pr_apim_alta_carteras_fisicas. '||SQLERRM,
                                    p_codigo_mail => 'MTEI');
            raise_application_error(-20181, 'Error al generar csv de altas. '||SQLERRM);
        END;
     END IF;
     
   END;

   PROCEDURE pr_apim_insert_cliente_antecedente(p_filas_insertadas OUT NUMBER)
   AS
   
   BEGIN
     BEGIN
       --FOR r IN () LOOP
       INSERT INTO ingres.sih_cliente_antecedente(nro_doc_id, cod_tipo_doc_id,
                                                  fec_actualizacion, primer_nombre, 
                                                  segundo_nombre, primer_apellido, 
                                                  segundo_apellido, apellido_casada,
                                                  tipo_control, cod_estado, 
                                                  tipo, tipo_movimiento, 
                                                  cod_cliente, fecha_final)
       SELECT c.nro_doc_id, c.cod_tipo_doc_id,
              SYSDATE, c.nombre1,
              substr(c.nombre2, 1, 15), c.apellido,
              substr(c.apellido2,1,20), c.apellido_casada,
              'EFECTIVO', 'EP',
              'WS', OPERACION,
              c.cod_cliente, ADD_MONTHS(SYSDATE, 30 * 12)
       FROM bfapim.temp_carteras_table c;
       
       p_filas_insertadas := SQL%ROWCOUNT;
       IF nvl(p_filas_insertadas, 0) > 0 THEN
         COMMIT;
         dbms_output.put_line(p_filas_insertadas || ' Registros insertados en sih_cliente_antecedente');
       END IF;
     EXCEPTION
       WHEN OTHERS THEN
         raise_application_error(-20032, 'Error en insert sih_cliente_antecedente. '||SQLERRM);
     END;
   END;
   
   
   PROCEDURE pr_apim_baja_carteras_fisicas
   AS
     archivo UTL_FILE.FILE_TYPE;
     linea VARCHAR2(32767);
     v_json_object_idbanco CLOB;
     v_id_banco_equifax VARCHAR2(200);
     v_nombre_archivo VARCHAR2(200);
     v_filas_insertadas NUMBER := 0;
   BEGIN
     --Insercion en tabla temporal
     BEGIN 
      DELETE bfapim.temp_carteras_table;
      INSERT INTO bfapim.temp_carteras_table(cod_cliente,     nro_doc_id, 
                                                  cod_tipo_doc_id, nombre1, 
                                                  nombre2,         apellido, 
                                                  apellido2,       apellido_casada, 
                                                  operacion, sexo)
      SELECT pf.cod_cliente,
             cl.nro_doc_id,
             cl.cod_tipo_doc_id,
             coalesce(pf.nombre1, cl.nombre) nombre1,
             TRIM(pf.nombre2) nombre2,
             coalesce(pf.apellido, ' ') apellido,
             coalesce(pf.apellido2, ' '),
             coalesce(pf.apellido_casada, ' '),
             'BA',
             pf.sexo
        FROM ingres.persona_fisica           pf,
             ingres.cliente                  cl,
             ingres.cliente_estadistica      ce,
             ingres.sih_clientes_controlados cc,
             ingres.resumen_mora_diario      rm,
             ingres.operacion                op,
             ingres.prestamo                 pr,
             ingres.supervisor_oficial       so
       WHERE pf.cod_cliente = cl.cod_cliente
         AND pf.cod_cliente = ce.cod_cliente
         AND pf.cod_cliente = cc.cod_cliente
         AND pf.cod_cliente = rm.cod_cliente
         AND rm.cod_sucursal = op.cod_sucursal
         AND rm.nro_operacion = op.nro_operacion
         AND rm.cod_sucursal = pr.cod_sucursal
         AND rm.nro_operacion = pr.nro_operacion
         AND ce.ult_cod_sucursal = op.cod_sucursal
         AND ce.ult_nro_operacion = op.nro_operacion
         AND cl.cod_supervisor = so.cod_supervisor
         AND cl.cod_oficial = so.cod_oficial
         AND cl.cod_tipo_doc_id = 1
         AND rm.venta_cartera = 1
         AND so.cod_tipo_oficial IN (0, 1, 2, 13)
         AND ((rm.cod_producto IN (1, 10) AND rm.area IN ('FA', 'OI')) OR
             (rm.cod_producto IN (22) AND rm.area IN ('MI')))
         AND (ce.cod_calificacion IN (40, 41, 42, 50, 51, 52) OR
             cl.cod_estado_cliente = 'BL' OR pr.ult_cuo_cobrada < 4 OR
             op.cod_destino_oper IN (42, 46, 49) OR op.cod_empresa = 3)
     UNION    
      SELECT pf.cod_cliente,
             cl.nro_doc_id,
             cl.cod_tipo_doc_id,
             coalesce(pf.nombre1, cl.nombre) nombre1,
             TRIM(pf.nombre2) nombre2,
             coalesce(pf.apellido, ' ') apellido,
             coalesce(pf.apellido2, ' '),
             coalesce(pf.apellido_casada, ' '),
             'BA',
             pf.sexo
        FROM ingres.persona_fisica           pf,
             ingres.cliente                  cl,
             ingres.cliente_estadistica      ce,
             ingres.sih_clientes_controlados cc,
             ingres.operacion                op
       WHERE pf.cod_cliente = cl.cod_cliente
         AND pf.cod_cliente = ce.cod_cliente
         AND pf.cod_cliente = cc.cod_cliente
         AND ce.ult_cod_sucursal = op.cod_sucursal
         AND ce.ult_nro_operacion = op.nro_operacion
         AND cl.cod_tipo_doc_id = 1
         AND ce.cant_prestamos_act = 0
         AND ce.cant_prestamos_canc > 0
         AND ((ce.fec_ult_canc < add_months(to_date(SYSDATE), -12) AND
             cod_producto <> 10) OR
             ce.fec_ult_canc < add_months(to_date(SYSDATE), -6) AND
             cod_producto IN (10))
         AND (ce.cod_calificacion IN (40, 41, 42, 50, 51, 52) OR
             cl.cod_estado_cliente = 'BL' OR
             op.cod_destino_oper IN (42, 46, 49) OR op.cod_empresa = 3);
     EXCEPTION
       WHEN OTHERS THEN
         pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error en inserci�n en tabla temporal', 
                                            'pr_apim_baja_carteras_fisicas', NULL);
         bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                p_texto => 'Error en inserci�n en tabla temporal, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                                p_codigo_mail => 'MTEI');
         raise_application_error(-20031, 'Error en inserci�n en tabla temporal. '||SQLERRM);
     END;
      
     -- Insercion en ingres.sih_cliente_antecedente a partir de la tabla temporal
     BEGIN
        pr_apim_insert_cliente_antecedente(v_filas_insertadas);
     EXCEPTION
       WHEN OTHERS THEN
         pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al insertar en sih_cliente_antecedente', 
                                            'pr_apim_baja_carteras_fisicas', NULL);
         bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al insertar en sih_cliente_antecedente, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
         dbms_output.put_line('Error al insertar en sih_cliente_antecedente. '||SQLERRM);
     END;
     
     IF v_filas_insertadas = 0 THEN
       pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Query de bajas no recupera ning�n registro', 
                                            'pr_apim_baja_carteras_fisicas', NULL);
       bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Query de bajas no recupera ning�n registro, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
       dbms_output.put_line('Query de bajas no recupera ning�n registro. '||SQLERRM);
     END IF;
     
     IF v_filas_insertadas > 0 THEN
        --Generar archivo csv
        BEGIN
          SELECT t.config_data INTO v_json_object_idbanco 
          FROM bfapim.apim_config t
          WHERE t.config_key = 'id_banco_equifax';
          v_id_banco_equifax := JSON_VALUE(v_json_object_idbanco, '$.id_banco');
        EXCEPTION
          WHEN OTHERS THEN
            raise_application_error(-20001, 'Error al obtener id_banco para equifax');
        END;
        v_nombre_archivo := 'PY_'||v_id_banco_equifax||'_Bajas_'||to_char(SYSDATE, 'DDMMYYYYHH24MISS');
        BEGIN
          archivo := UTL_FILE.FOPEN('ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX', v_nombre_archivo||'.csv', 'W');
          --linea := 'Documento,Tipo Documento,Nombre/Raz�n Social,Sexo,Cartera,Calle,Altura,Piso y Dpto,Localidad,C�digo Postal,Provincia,Tel�fono,Operaci�n,Tiempo de Control,Ejecutivo,Sucursal,Tipo Cliente';
          linea := 'Documento;Tipo Documento;Primer nombre;Segundo nombre;Primer apellido;Segundo apellido;Sexo;Cartera;Calle;Altura;Piso y Depto;Localidad;C�digo Postal;Provincia;Tel�fono;Operaci�n;Tiempo de Control;Ejecutivo;Sucursal;Tipo de Cliente';
          UTL_FILE.PUT_LINE(archivo, linea);
          FOR fila IN (SELECT x.cod_cliente, x.nro_doc_id, x.cod_tipo_doc_id, 
                           x.nombre1, TRIM(x.nombre2) nombre2, x.apellido, 
                           x.apellido2, x.apellido_casada, 
                           x.operacion, x.sexo FROM bfapim.temp_carteras_table x) LOOP
            /*linea := fila.nro_doc_id || ',' || 'CI' || ',' || fila.nombre1 || CASE WHEN fila.nombre2 IS NOT NULL  
                                                                          THEN ' '||fila.nombre2
                                                                          ELSE ''
                                                                          END ||
                 ' '||fila.apellido || ',' || fila.sexo ||',,,,,,,,,A,,,,' ;*/
              linea := fila.nro_doc_id || ';' || 'CI' || ';' || fila.nombre1 || ';' || fila.nombre2 || ';' || fila.apellido || ';' || fila.apellido2 || ';' || fila.sexo ||';;;;;;;;;B;;;;' ;
            UTL_FILE.PUT_LINE(archivo, linea);
          END LOOP;
          UTL_FILE.FCLOSE(archivo);
        EXCEPTION
          WHEN OTHERS THEN
            pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al generar archivo csv para bajas de cartera', 
                                            'pr_apim_baja_carteras_fisicas', NULL);
            bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                    p_texto => 'Error al generar archivo csv para bajas de cartera, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                                    p_codigo_mail => 'MTEI');
            raise_application_error(-20185, 'Error al generar csv de bajas. '||SQLERRM);
        END;
     END IF;
   END;
   
  /*PROCEDURE pr_apim_insert_apim_archivos_equifax(p_nombre IN VARCHAR2, p_fecha IN DATE, p_estado IN VARCHAR2, p_tipo IN VARCHAR2)
  AS
  BEGIN
    INSERT INTO bfapim.APIM_ARCHIVOS_EQUIFAX(nombre, fecha, estado, tipo)
    VALUES (p_nombre, p_fecha, p_estado, p_tipo);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      raise_application_error(-20034, 'Error en insert en APIM_ARCHIVOS_EQUIFAX. '||SQLERRM);
  END;*/
  
  FUNCTION pr_apim_2array2value (p_line VARCHAR2, p_position IN NUMBER) RETURN VARCHAR2
  IS
    v_result VARCHAR2(200);
    TYPE StringArray IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;
   -- Variables
   v_array   StringArray;
   v_string  VARCHAR2(500);
   v_temp    VARCHAR2(100);
   v_index   PLS_INTEGER := 1;
  BEGIN
     v_string := p_line;
   -- Bucle para dividir la cadena por el delimitador
   WHILE INSTR(v_string, ';') > 0 LOOP
      -- Extraer el primer valor antes del delimitador
      v_temp := SUBSTR(v_string, 1, INSTR(v_string, ';') - 1);
      
      -- Almacenar el valor en el array
      v_array(v_index) := v_temp;

      -- Ajustar la cadena eliminando el valor procesado
      v_string := SUBSTR(v_string, INSTR(v_string, ';') + 1);

      -- Incrementar el �ndice del array
      v_index := v_index + 1;
   END LOOP;

   -- Almacenar el �ltimo valor restante (sin delimitador)
   v_array(v_index) := v_string;

   -- Imprimir los valores del array
   --FOR i IN 1 .. v_array.COUNT LOOP
      DBMS_OUTPUT.PUT_LINE('Elemento '||  p_position || ': ' || v_array(p_position));
      v_result := v_array(p_position);
   --END LOOP;
    RETURN v_result;
  END;
  
  PROCEDURE pr_apim_insert_cliente_verificar(p_cod_cliente IN VARCHAR2, p_nro_doc_id IN VARCHAR2, 
                                            p_cod_tipo_doc_id IN NUMBER, p_nombre IN VARCHAR2, 
                                            p_apellido IN VARCHAR2, p_tipo_control IN VARCHAR2,
                                            p_tipo_movimiento IN VARCHAR2, p_cod_estado IN VARCHAR2,
                                            p_fec_actualizacion IN DATE, p_comentario IN VARCHAR2)
  AS
  
  BEGIN
    INSERT INTO ingres.sih_cliente_verificar (cod_cliente,    nro_doc_id, 
                                             cod_tipo_doc_id, nombre,      
                                             apellido,        tipo_control, 
                                             tipo_movimiento, cod_estado, 
                                             fec_actualizacion, comentario)
                                       VALUES(p_cod_cliente,    p_nro_doc_id, 
                                            p_cod_tipo_doc_id,  p_nombre, 
                                            p_apellido,         p_tipo_control,
                                            p_tipo_movimiento,  p_cod_estado,
                                            p_fec_actualizacion, p_comentario);
  EXCEPTION
    WHEN OTHERS THEN
      raise_application_error(-20035, 'Error en insert en sih_cliente_verificar. '||SQLERRM);
  END;
  
   
   
   PROCEDURE pr_apim_cartera_confirmados
   AS
     file_handle UTL_FILE.FILE_TYPE;
     file_line       VARCHAR2(32767);
     archivo_entrada UTL_FILE.FILE_TYPE;
     linea           varchar2(32767);
     v_path_confirmados varchar2(200) := 'ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX$CONFIRMADOS';
     
     v_nombre1     VARCHAR2(65);
     v_nombre2     VARCHAR2(65);
     v_apellido   VARCHAR2(20);
     v_apellido2   VARCHAR2(20);
     v_apellido_casada   VARCHAR2(20);
     v_tipo_control char(8);
     v_tipo_doc_str VARCHAR2(10);
     v_tipo_doc_num NUMBER;
     v_fecha_nombre_archivo VARCHAR2(20);
     v_fecha_inicio DATE;
     v_fecha_final DATE;
     v_cliente_controlado VARCHAR2(2);
     v_cod_cliente VARCHAR2(11);
     v_rowcount NUMBER := 0;
     v_file_name     VARCHAR2(255);
     v_nro_doc    VARCHAR2(100);
     v_esAlta BOOLEAN := FALSE;
     v_dioError BOOLEAN;
   BEGIN
     BEGIN
       DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS');
       DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS');
       dbms_output.put_line('TERMINADO RUN JOB LIST_FILES_EQUIFAX_CONFIRMADOS');
     EXCEPTION
       WHEN OTHERS THEN
         pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al ejecutar Job LIST_FILES_EQUIFAX_CONFIRMADOS', 
                                            'pr_apim_cartera_confirmados', NULL);
         bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al ejecutar Job LIST_FILES_EQUIFAX_CONFIRMADOS, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
         raise_application_error(-20120, 'Error en JOB LIST_FILES_EQUIFAX_CONFIRMADOS. '||SQLERRM);
     END;
     -- Abrir el archivo de salida (donde se encuentra la lista de archivos obtenidos)
     archivo_entrada := utl_file.fopen(v_path_confirmados, 'output_mt.txt', 'R');
     DBMS_OUTPUT.ENABLE(1000000); -- Establece el tama�o del b�fer en 1 MB
     LOOP
        BEGIN
          utl_file.get_line(archivo_entrada, linea);
          v_file_name := linea; --para tener ordenado el codigo se guarda en la variable

          IF v_file_name LIKE '%PROCESADO%' THEN
            IF v_file_name LIKE '%_Altas_%' THEN
              v_fecha_nombre_archivo := SUBSTR(v_file_name, instr(v_file_name, '_Altas_') + 7, 8);
              v_esAlta := TRUE;
            END IF;
            
            dbms_output.put_line('Procesando: '|| v_file_name);
            /*Tratamiento del o de los archivos csv de rechazados*/
            file_handle := UTL_FILE.FOPEN(v_path_confirmados, v_file_name, 'R');
            LOOP
              BEGIN
                  -- Leer una l�nea del archivo
                  UTL_FILE.GET_LINE(file_handle, file_line);
                  v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 1);
                  
                  IF v_nro_doc != 'Documento' THEN
                    v_dioError := FALSE;
                    -- Separar los valores del CSV (usualmente separados por , o ;)
                    v_tipo_doc_str := pr_apim_2array2value (file_line, 2);
                    DBMS_OUTPUT.PUT_LINE( 'Documento a procesar:' || v_nro_doc||' - v_tipo_doc_str: '|| v_tipo_doc_str);
                    
                    -- OBTENER EQUIVALENCIA DEL TIPO DE DOCUMENTO
                    /*IF v_tipo_doc_str = 'CI' THEN
                      v_tipo_doc_num := 1;
                    ELSIF v_tipo_doc_str = 'RUC' THEN
                      v_tipo_doc_num := 4;
                    END IF;*/
                    BEGIN
                      SELECT cod_tipo_doc_id INTO v_tipo_doc_num
                      from ingres.tipo_documento_id
                      WHERE TRIM(abrev_tipo_doc_id) = TRIM(v_tipo_doc_str);
                    EXCEPTION
                      WHEN OTHERS THEN
                        v_dioError := TRUE;
                        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al recuperar tipo documento de ingres.tipo_documento_id.', 
                                            'pr_apim_cartera_confirmados', NULL);
                        bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al recuperar tipo documento de ingres.tipo_documento_id, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                        DBMS_OUTPUT.PUT_LINE('Error al recuperar tipo documento. '||v_tipo_doc_str||' '||SQLERRM);
                    END;

                    IF v_esAlta = TRUE AND v_dioError = FALSE THEN
                      -- ACTUALIZAR CLIENTE_ANTECEDENTE
                      BEGIN
                          UPDATE ingres.sih_cliente_antecedente
                          SET cod_estado = 'PR',
                              fec_actualizacion = SYSDATE
                          WHERE nro_doc_id = v_nro_doc
                            AND cod_tipo_doc_id = v_tipo_doc_num
                            AND TRUNC(fec_actualizacion) = to_date(v_fecha_nombre_archivo, 'DDMMYYYY');
                            dbms_output.put_line('sih_cliente_antecedente actualizado '||v_nro_doc); 
                      EXCEPTION
                        WHEN OTHERS THEN
                          v_dioError := TRUE;
                          pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al actualizar ingres.sih_cliente_antecedente', 
                                            'pr_apim_cartera_confirmados', NULL);
                          bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al actualizar ingres.sih_cliente_antecedente, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                          DBMS_OUTPUT.PUT_LINE('Error al actualizar ingres.sih_cliente_antecedente. '||SQLERRM);
                          --raise_application_error(-20001, 'Error al actualizar en ingres.sih_cliente_antecedente. '||SQLERRM);
                      END;
                      v_rowcount := v_rowcount + SQL%ROWCOUNT;
                      
                      -- OBTENER CODIGO DE CLIENTE Y OTROS DATOS DEL CLIENTE
                      BEGIN
                        SELECT primer_nombre, segundo_nombre, primer_apellido, segundo_apellido, apellido_casada, tipo_control, fecha_final, cod_cliente
                        INTO v_nombre1, v_nombre2, v_apellido, v_apellido2, v_apellido_casada, v_tipo_control, v_fecha_final, v_cod_cliente
                        FROM ingres.sih_cliente_antecedente
                        WHERE nro_doc_id = v_nro_doc
                          AND cod_tipo_doc_id = v_tipo_doc_num
                          AND trunc(fec_actualizacion) = trunc(SYSDATE)
                          AND ROWNUM = 1;
                      EXCEPTION
                        WHEN OTHERS THEN
                        v_dioError := TRUE;
                        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al recuperar datos de sih_cliente_antecedente', 
                                            'pr_apim_cartera_confirmados', NULL);
                        bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Error al recuperar datos de sih_cliente_antecedente, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
                        DBMS_OUTPUT.PUT_LINE('Error al recuperar en sih_cliente_antecedente. '||v_nro_doc||' '||SQLERRM);
                      END;
                      v_cliente_controlado := 'SI';
                      v_fecha_inicio := SYSDATE;
                      
                      -- SE INSERTA EN SIH_CLIENTES_CONTROLADOS
                      IF v_dioError = FALSE THEN
                        BEGIN
                          pr_apim_insert_clientes_controlados(v_nro_doc, v_nombre1, v_nombre2, v_apellido, v_apellido2, v_apellido_casada, v_tipo_control, v_fecha_inicio, v_fecha_final, v_cliente_controlado, v_cod_cliente);
                        EXCEPTION
                          WHEN OTHERS THEN
                            v_dioError := TRUE;
                            pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error al insertar en sih_cliente_controlados', 
                                            'pr_apim_cartera_confirmados', NULL);
                            bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                  p_texto => 'Error al insertar en sih_cliente_controlados, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                                  p_codigo_mail => 'MTEI');
                            DBMS_OUTPUT.PUT_LINE('Error al insertar en sih_cliente_controlados. '||SQLERRM);
                        END;
                      END IF;
                    ELSE -- Aqui entraria cuando se trata de un PROCESADO BAJAS
                      IF v_dioError = FALSE THEN
                        -- DELETE DE sih_clientes_controlados
                        BEGIN
                          DELETE ingres.sih_clientes_controlados
                          WHERE nro_doc_id = v_nro_doc;
                          v_rowcount := SQL%ROWCOUNT;
                        EXCEPTION
                          WHEN OTHERS THEN
                          v_dioError := TRUE;
                          pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                              SQLERRM, 'Error al borrar sih_cliente_controlados', 
                                              'pr_apim_cartera_confirmados', NULL);
                          bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                p_texto => 'Error al borrar sih_cliente_controlados, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                                p_codigo_mail => 'MTEI');
                          DBMS_OUTPUT.PUT_LINE('Error al borrar sih_cliente_controlados. '||SQLERRM);
                        END;
                      END IF;
                    END IF;
                  END IF;
              EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                      -- Finalizar el bucle cuando no haya m�s datos en el archivo
                      EXIT;
              END;
            END LOOP;
            UTL_FILE.FCLOSE(file_handle); --cierra el csv de confirmados
            
            IF v_rowcount > 0 AND v_dioError = FALSE THEN
              -- Correr script bash que mueve el archivo procesado
              DBMS_OUTPUT.PUT_LINE( 'Moviendo documento '|| v_file_name);
              pr_mover_archivo('/archivos_aplicacion/entrada/otras_instituciones/equifax/confirmados/'||v_file_name, '/archivos_aplicacion/entrada/otras_instituciones/equifax/procesados/');
              DBMS_OUTPUT.PUT_LINE( 'Documento movido a: ' || '/archivos_aplicacion/entrada/otras_instituciones/equifax/procesados/');
              COMMIT;
            END IF;
            
          END IF;
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya m�s datos en el archivo
                  EXIT;
        END;
      END LOOP;
      UTL_FILE.FCLOSE(archivo_entrada);
     
   END; -- pr_apim_cartera_confirmados
   
   PROCEDURE pr_apim_insert_clientes_controlados(nro_doc_id  IN VARCHAR2, nombre1  IN VARCHAR2, 
                                               nombre2  IN VARCHAR2, apellido IN VARCHAR2,
                                               apellido2 IN VARCHAR2, apellido_casada IN VARCHAR2,
                                               tipo_control IN VARCHAR2, fecha_inicio IN DATE, 
                                               fecha_final IN DATE,  cliente_controlado IN VARCHAR2, 
                                               cod_cliente IN VARCHAR2)
   AS
   BEGIN
     INSERT INTO ingres.sih_clientes_controlados (nro_doc_id, nombre1, nombre2, 
                                                  apellido, apellido2, apellido_casada, 
                                                  tipo_control, fecha_inicio, fecha_final, 
                                                  cliente_controlado, cod_cliente )
                                          VALUES (nro_doc_id, nombre1, nombre2, 
                                                  apellido, apellido2, apellido_casada,
                                                  tipo_control, fecha_inicio, fecha_final,  
                                                  cliente_controlado, cod_cliente);
   EXCEPTION
     WHEN OTHERS THEN
       raise_application_error(-20038, 'Error en insert en sih_clientes_controlados. '||SQLERRM);
   END;
   
   
   PROCEDURE pr_apim_insert_apim_equifax_log(p_hora_evento IN date, p_sqlcode IN VARCHAR2,
                                            p_sqlerrm IN VARCHAR2, p_mensaje IN varchar2, 
                                            p_from_objecto IN varchar2, p_data IN CLOB)
   AS
   PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
     INSERT INTO bfapim.apim_equifax_log(hora_evento, sqlcode, 
                                         sqlerrm,     mensaje, 
                                         from_objecto, data)
                                  VALUES(p_hora_evento,  p_sqlcode,
                                         p_sqlerrm,      p_mensaje, 
                                         p_from_objecto, p_data);
     COMMIT;
   EXCEPTION
     WHEN OTHERS THEN
        --raise_application_error(-20050, 'Error en insert en bfapim.apim_equifax_log. '||SQLERRM);
        dbms_output.put_line('Error en insert en bfapim.apim_equifax_log. '||SQLERRM);
   END;
   
   PROCEDURE pr_apim_ejecuta_confirm_rechazado
     AS
     BEGIN
       pr_apim_cartera_confirmados;
       pr_apim_cartera_rechazados;
     END;
END;
/
