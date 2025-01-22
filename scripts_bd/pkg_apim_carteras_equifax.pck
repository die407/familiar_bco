CREATE OR REPLACE PACKAGE BFAPIM.pkg_apim_carteras_equifax
IS
 v_scripts_directory_rechazados constant varchar2(100) := 'ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$SALIDA$EQUIFAX';
 v_nombre_ejecutable   constant varchar2(100) := 'get_files_1.sh';
 TYPE nested_table_type IS TABLE OF VARCHAR2(100); -- Declarar el tipo de tabla anidada
 my_nested_table nested_table_type; -- Declarar la variable de tabla anidada

 PROCEDURE pr_apim_cartera_rechazados;

 PROCEDURE pr_swf_crea_get_file_sh;

 PROCEDURE pr_mover_archivo(p_archivo_origen IN VARCHAR2, p_destino VARCHAR2);

 PROCEDURE pr_crea_script_sh(p_config_key IN VARCHAR2, p_directorio_destino IN VARCHAR2, p_archivo_nombre IN VARCHAR2);

 PROCEDURE pr_apim_alta_carteras_fisicas;

 PROCEDURE pr_apim_insert_cliente_antecedente(p_filas_insertadas OUT NUMBER);

 PROCEDURE pr_apim_baja_carteras_fisicas;

 PROCEDURE pr_apim_insert_apim_archivos_equifax(p_nombre IN VARCHAR2, p_fecha IN DATE, p_estado IN VARCHAR2, p_tipo IN VARCHAR2);

 FUNCTION pr_apim_2array2value (p_line VARCHAR2, p_position IN NUMBER) RETURN VARCHAR2;

 PROCEDURE pr_apim_insert_cliente_verificar(p_cod_cliente IN VARCHAR2, p_nro_doc_id IN VARCHAR2,
                                            p_cod_tipo_doc_id IN NUMBER, p_nombre IN VARCHAR2,
                                            p_apellido IN VARCHAR2, p_tipo_control IN VARCHAR2,
                                            p_tipo_movimiento IN VARCHAR2, p_cod_estado IN VARCHAR2,
                                            p_fec_actualizacion IN DATE, p_comentario IN VARCHAR2,
                                            P_INSERTADO OUT BOOLEAN, P_DIOERROR OUT BOOLEAN);

 PROCEDURE pr_apim_cartera_confirmados;

 PROCEDURE pr_apim_insert_clientes_controlados(nro_doc_id  IN VARCHAR2, nombre1  IN VARCHAR2,
                                               nombre2  IN VARCHAR2, apellido IN VARCHAR2,
                                               apellido2 IN VARCHAR2, apellido_casada IN VARCHAR2,
                                               tipo_control IN VARCHAR2, fecha_inicio IN DATE,
                                               fecha_final IN DATE,  cliente_controlado IN VARCHAR2,
                                               cod_cliente IN VARCHAR2, P_DIOERROR OUT BOOLEAN);

  PROCEDURE pr_apim_insert_apim_equifax_log(p_hora_evento IN date, p_sqlcode IN VARCHAR2,
                                            p_sqlerrm IN VARCHAR2, p_mensaje IN varchar2,
                                            p_from_objecto IN varchar2, p_data IN CLOB);

  PROCEDURE pr_apim_ejecuta_confirm_rechazado;

  FUNCTION fc_apim_existe_archivos_equifax(p_nombre_archivo VARCHAR2) RETURN BOOLEAN;

  PROCEDURE pr_apim_elimina_clientes_controlados(P_DOCUMENTO IN VARCHAR2, P_COD_CLIENTE IN VARCHAR2 DEFAULT NULL, P_DIOERROR OUT BOOLEAN);

  PROCEDURE pr_apim_list_rep_rechazados;

  PROCEDURE pr_apim_list_rechazado;

  PROCEDURE pr_apim_obt_cliente_antecedente(P_NRO_DOC IN VARCHAR2, P_TIPO_DOC_NUM IN NUMBER, P_FECHA IN DATE,
                                             P_ROW_CLIENTE_ANT OUT ingres.sih_cliente_antecedente%ROWTYPE,
                                             P_DIOERROR OUT BOOLEAN);

  PROCEDURE pr_apim_obt_equivalencia_tipo_doc(P_TIPO_DOC_STR IN VARCHAR2, P_EQUIVALENCIA OUT NUMBER, P_DIOERROR OUT BOOLEAN);

  FUNCTION fn_apim_existe_cliente_verificar(P_COD_CLIENTE VARCHAR2) RETURN NUMBER;

  PROCEDURE pr_apim_update_cliente_verificar(P_COD_CLIENTE IN VARCHAR2, P_FEC_ACTUALIZACION IN DATE, P_COMENTARIO IN VARCHAR2,
                                              P_INSERTADO OUT BOOLEAN, P_DIOERROR OUT BOOLEAN);

  PROCEDURE pr_apim_update_cliente_antecedente(P_NRO_DOC IN VARCHAR2, P_TIPO_DOC_NUM IN NUMBER, P_FEC_ACTUALIZACION IN DATE,
                                               P_COD_ESTADO_UPD IN VARCHAR2, P_COMENTARIO_UPD IN VARCHAR2, P_FEC_UPD IN DATE,
                                               P_DIOERROR OUT BOOLEAN);

  PROCEDURE pr_apim_obt_comment_tipo_doc(P_LINEA IN VARCHAR2, P_COMMENT OUT VARCHAR2, P_TIPO_DOC_STR OUT VARCHAR2, P_DIOERROR OUT BOOLEAN);

  PROCEDURE pr_apim_update_ca(P_NRO_DOC IN VARCHAR2, P_COD_TIPO_DOC_NUM IN NUMBER, P_COMENTARIO IN VARCHAR2,
                               P_DIOERROR OUT BOOLEAN, P_ROWCOUNT OUT NUMBER);

  PROCEDURE pr_apim_obt_fecha_esalta(P_FILENAME IN VARCHAR2, P_FECHA_NOMARCHIVO OUT VARCHAR2, P_ESALTA OUT BOOLEAN);
END;
/
CREATE OR REPLACE PACKAGE BODY BFAPIM.pkg_apim_carteras_equifax
IS

   PROCEDURE pr_apim_cartera_rechazados
   IS
     file_handle UTL_FILE.FILE_TYPE;
     file_line       VARCHAR2(32767);
     archivo_entrada UTL_FILE.FILE_TYPE;
     linea           varchar2(32767);
     v_nro_doc    VARCHAR2(100);
     v_comentario VARCHAR2(200);
     v_tipo_doc_str VARCHAR2(20);
     v_tipo_doc_num NUMBER;
     v_fec_actualizacion DATE;
     v_rowcount NUMBER := 0;

     v_file_name     VARCHAR2(255);
     v_fecha_nombre_archivo VARCHAR2(20);
     v_dioError BOOLEAN;
     v_se_inserto BOOLEAN := FALSE;
     v_cant_error NUMBER;
     v_esAlta BOOLEAN := FALSE;
     v_sih_cliente_antecedente_row ingres.sih_cliente_antecedente%ROWTYPE;
     v_rowcount_uno NUMBER := 0;
     v_existe_cv NUMBER := 0;
   BEGIN
    dbms_output.put_line('INICIO PROCESO ReporteRechazos');
    BEGIN

      BEGIN
        --ejecuta el archivo .sh que crea el archivo output_mt.txt que contiene el listado de archivos en el directorio
        --DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS');
        DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS');
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -27369 THEN
            dbms_output.put_line('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS. Sin archivos para listar. '||SQLERRM);
            RETURN;
          ELSE
            pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                              SQLERRM, 'Error al ejecutar Job LIST_FILES_EQUIFAX_RECHAZOS. '||SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2),
                                              'pr_apim_cartera_rechazados', NULL);
            bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                p_texto => 'Error al ejecutar Job LIST_FILES_EQUIFAX_RECHAZOS, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_rechazados. Ejecución ABORTADA. '||SQLERRM,
                                p_codigo_mail => 'MTEI');
            dbms_output.put_line('Error en JOB. Procedimiento ABORTADO. '||SQLERRM || ' sqlcode: '||SQLCODE);
            RETURN;
          END IF;
      END;
      dbms_output.put_line('TERMINADO RUN_JOB para listar archivos csv');
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
            v_esAlta := FALSE;
            IF fc_apim_existe_archivos_equifax(v_file_name) = FALSE THEN
                pr_apim_obt_fecha_esalta(v_file_name, v_fecha_nombre_archivo, v_esAlta);

                dbms_output.put_line('Procesando: '|| v_file_name || ' Fecha: '||v_fecha_nombre_archivo);

                /*Tratamiento del o de los archivos csv de rechazados*/
                file_handle := UTL_FILE.FOPEN(v_scripts_directory_rechazados, v_file_name, 'R');
                v_cant_error := 0;
                LOOP
                  v_dioError := FALSE;
                  BEGIN
                      -- Leer una línea del archivo
                      UTL_FILE.GET_LINE(file_handle, file_line);
                      v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 2);

                      IF v_nro_doc != 'Documento' THEN
                        -- Separar los valores del CSV (usualmente separados por , o ;)
                        pr_apim_obt_comment_tipo_doc(file_line, v_comentario,
                                                     v_tipo_doc_str, v_dioError);

                        -- OBTENER EQUIVALENCIA DEL TIPO DE DOCUMENTO
                        IF NVL(v_dioError, FALSE) = FALSE THEN
                          pr_apim_obt_equivalencia_tipo_doc(v_tipo_doc_str, v_tipo_doc_num, v_dioError);
                        END IF;

                        -- ACTUALIZAR CLIENTE_ANTECEDENTE
                        --IF NVL(v_dioError, FALSE) = FALSE THEN
                        IF (v_esAlta = TRUE AND NVL(v_dioError, FALSE) = FALSE AND v_comentario != 'Error de validacion: Ya esta en seguimiento') OR (v_esAlta = FALSE AND NVL(v_dioError, FALSE) = FALSE) THEN
                          pr_apim_update_ca(v_nro_doc, v_tipo_doc_num, v_comentario, v_dioError, v_rowcount_uno);
                          v_rowcount := v_rowcount + v_rowcount_uno;

                        END IF;

                        -- OBTENER CODIGO DE CLIENTE Y OTROS DATOS
                        IF NVL(v_dioError, FALSE) = FALSE THEN
                            pr_apim_obt_cliente_antecedente(v_nro_doc, v_tipo_doc_num, trunc(SYSDATE),
                            v_sih_cliente_antecedente_row, v_dioError);
                            v_fec_actualizacion := SYSDATE;

                        END IF;

                        -- Eliminar de sih_clientes_controlados
                        IF NVL(v_dioError, FALSE) = FALSE AND v_esAlta = TRUE AND v_comentario != 'Error de validacion: Ya esta en seguimiento' THEN
                            pr_apim_elimina_clientes_controlados(v_nro_doc, v_sih_cliente_antecedente_row.cod_cliente, v_dioError);

                        END IF;

                          -- Se inserta en sih_cliente_verificar o se actualiza
                          IF NVL(v_dioError, FALSE) = FALSE THEN
                              v_existe_cv := 0;
                              v_existe_cv := fn_apim_existe_cliente_verificar(v_sih_cliente_antecedente_row.cod_cliente);

                              IF v_existe_cv = 1 THEN
                                  pr_apim_update_cliente_verificar(v_sih_cliente_antecedente_row.cod_cliente, v_fec_actualizacion, v_comentario, v_se_inserto, v_dioError);

                              ELSE
                                  pr_apim_insert_cliente_verificar(v_sih_cliente_antecedente_row.cod_cliente,       v_nro_doc,
                                                                   v_tipo_doc_num,      v_sih_cliente_antecedente_row.primer_nombre,
                                                                   v_sih_cliente_antecedente_row.primer_apellido,          v_sih_cliente_antecedente_row.tipo_control,
                                                                   v_sih_cliente_antecedente_row.tipo_movimiento,   v_sih_cliente_antecedente_row.cod_estado,
                                                                   v_fec_actualizacion, v_comentario,
                                                                   v_se_inserto, v_dioError);

                              END IF;
                          END IF;
                          -- Contabilizamos cada linea con error
                          IF NVL(v_dioError, FALSE) = TRUE THEN
                            v_cant_error := v_cant_error + 1;

                          END IF;
                        --END IF;
                      END IF;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                          -- Finalizar el bucle cuando no haya más datos en el archivo
                          EXIT;
                  END;
                END LOOP;
                UTL_FILE.FCLOSE(file_handle); --cierra el csv de rechazados

                --IF v_rowcount > 0 THEN
                IF v_se_inserto = TRUE THEN
                  pr_apim_insert_apim_archivos_equifax(v_file_name, SYSDATE, 'PROCESADO', CASE
                                                        WHEN instr(v_file_name, '_Altas_') > 0 THEN 'ALTA'
                                                        ELSE 'BAJA'
                                                    END);
                  COMMIT;

                END IF;
                -- Correr script bash que mueve el archivo procesado
                DBMS_OUTPUT.PUT_LINE( 'Moviendo documento '|| v_file_name);
                pr_mover_archivo('/archivos_aplicacion/salida/otras_instituciones/equifax/'||v_file_name, '/archivos_aplicacion/salida/otras_instituciones/equifax/procesados/');
                DBMS_OUTPUT.PUT_LINE( 'Documento movido a: ' || '/archivos_aplicacion/salida/otras_instituciones/equifax/procesados/');

                IF v_cant_error > 0 THEN
                  bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                      p_texto => v_cant_error||' Documentos no procesados, revise el log equifax. Archivo: '||v_file_name,
                                      p_codigo_mail => 'MTEI');
                END IF;

            END IF; -- fc_apim_existe_archivos_equifax
          END IF;
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya más datos en el archivo
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

      -- Definir los parámetros
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

      -- No es necesario Habilitar el Job
      --DBMS_SCHEDULER.ENABLE('BFAPIM.JOB_MOVER_ARCHIVO_REC_PROC');

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


   PROCEDURE pr_apim_alta_carteras_fisicas
   AS
     archivo UTL_FILE.FILE_TYPE;
     linea VARCHAR2(32767);
     v_json_object_idbanco CLOB;
     v_id_banco_equifax VARCHAR2(200);
     v_nombre_archivo VARCHAR2(200);
     v_filas_insertadas NUMBER := 0;
     --v_dioError BOOLEAN;

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

         raise_application_error(-20031, 'Error en inserción '||SQLERRM);
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
                                            SQLERRM, 'Query de altas no recupera ningún registro',
                                            'pr_apim_alta_carteras_fisicas', NULL);
       bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Query de altas no recupera ningún registro, en bfapim.pkg_apim_carteras_equifax.pr_apim_alta_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
       dbms_output.put_line('Query de altas no recupera ningún registro. '||SQLERRM);
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
          linea := 'Documento;Tipo Documento;Primer nombre;Segundo nombre;Primer apellido;Segundo apellido;Sexo;Cartera;Calle;Altura;Piso y Depto;Localidad;Código Postal;Provincia;Teléfono;Operación;Tiempo de Control;Ejecutivo;Sucursal;Tipo de Cliente';
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
     v_json_object_cant_dias CLOB;
     v_id_banco_equifax VARCHAR2(200);
     v_cant_dias_bajas NUMBER := 0;
     v_nombre_archivo VARCHAR2(200);
     v_filas_insertadas NUMBER := 0;
   BEGIN
     -- Obtener la cantidad de dias para query de bajas equifax
     BEGIN
         SELECT t.config_data INTO v_json_object_cant_dias
         FROM bfapim.apim_config t
         WHERE t.config_key = 'cant_dias_bajas_equifax';
         v_cant_dias_bajas := JSON_VALUE(v_json_object_cant_dias, '$.cant_dias');
     EXCEPTION
         WHEN OTHERS THEN
           pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                          SQLERRM, 'Error al obtener cantidad días baja para equifax',
                                          'pr_apim_baja_carteras_fisicas', NULL);
           bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                  p_texto => 'Error al obtener cantidad de días baja para equifax, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                                  p_codigo_mail => 'MTEI');
           raise_application_error(-20183, 'Error al obtener cantidad días baja para equifax');
     END;

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
         AND (TRUNC(SYSDATE) - TRUNC(cc.fecha_inicio)) > v_cant_dias_bajas
         --AND (TRUNC(SYSDATE) - TRUNC(cc.fecha_inicio)) < 25
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
             op.cod_destino_oper IN (42, 46, 49) OR op.cod_empresa = 3)
         AND (TRUNC(SYSDATE) - TRUNC(cc.fecha_inicio)) > v_cant_dias_bajas;
          --AND (TRUNC(SYSDATE) - TRUNC(cc.fecha_inicio)) < 25;
     EXCEPTION
       WHEN OTHERS THEN
         pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                            SQLERRM, 'Error en inserción en tabla temporal',
                                            'pr_apim_baja_carteras_fisicas', NULL);
         bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                p_texto => 'Error en inserción en tabla temporal, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                                p_codigo_mail => 'MTEI');
         raise_application_error(-20031, 'Error en inserción en tabla temporal. '||SQLERRM);
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
                                            SQLERRM, 'Query de bajas no recupera ningún registro',
                                            'pr_apim_baja_carteras_fisicas', NULL);
       bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                              p_texto => 'Query de bajas no recupera ningún registro, en bfapim.pkg_apim_carteras_equifax.pr_apim_baja_carteras_fisicas. '||SQLERRM,
                              p_codigo_mail => 'MTEI');
       dbms_output.put_line('Query de bajas no recupera ningún registro. '||SQLERRM);
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
          --linea := 'Documento,Tipo Documento,Nombre/Razón Social,Sexo,Cartera,Calle,Altura,Piso y Dpto,Localidad,Código Postal,Provincia,Teléfono,Operación,Tiempo de Control,Ejecutivo,Sucursal,Tipo Cliente';
          linea := 'Documento;Tipo Documento;Primer nombre;Segundo nombre;Primer apellido;Segundo apellido;Sexo;Cartera;Calle;Altura;Piso y Depto;Localidad;Código Postal;Provincia;Teléfono;Operación;Tiempo de Control;Ejecutivo;Sucursal;Tipo de Cliente';
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

  PROCEDURE pr_apim_insert_apim_archivos_equifax(p_nombre IN VARCHAR2, p_fecha IN DATE, p_estado IN VARCHAR2, p_tipo IN VARCHAR2)
  AS
  BEGIN
    INSERT INTO bfapim.APIM_ARCHIVOS_EQUIFAX(nombre, fecha, estado, tipo)
    VALUES (p_nombre, p_fecha, p_estado, p_tipo);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      raise_application_error(-20034, 'Error en insert en APIM_ARCHIVOS_EQUIFAX. '||SQLERRM);
  END;

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

      -- Incrementar el índice del array
      v_index := v_index + 1;
   END LOOP;

   -- Almacenar el último valor restante (sin delimitador)
   v_array(v_index) := v_string;

   -- Imprimir los valores del array
   --FOR i IN 1 .. v_array.COUNT LOOP
      --DBMS_OUTPUT.PUT_LINE('Elemento '||  p_position || ': ' || v_array(p_position));
      v_result := v_array(p_position);
   --END LOOP;
    RETURN v_result;
  END;

  PROCEDURE pr_apim_insert_cliente_verificar(p_cod_cliente IN VARCHAR2, p_nro_doc_id IN VARCHAR2,
                                            p_cod_tipo_doc_id IN NUMBER, p_nombre IN VARCHAR2,
                                            p_apellido IN VARCHAR2, p_tipo_control IN VARCHAR2,
                                            p_tipo_movimiento IN VARCHAR2, p_cod_estado IN VARCHAR2,
                                            p_fec_actualizacion IN DATE, p_comentario IN VARCHAR2,
                                            P_INSERTADO OUT BOOLEAN, P_DIOERROR OUT BOOLEAN)
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
    IF SQL%ROWCOUNT > 0 THEN
      P_INSERTADO := TRUE;
    ELSE
      P_INSERTADO := FALSE;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      P_DIOERROR := TRUE;
      P_INSERTADO := FALSE;
      pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                            SQLERRM,'Error al insertar datos en sih_cliente_verificar. ' || SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2),
                            'pr_apim_insert_cliente_verificar', NULL);
      dbms_output.put_line('Error al insertar cliente_verificar. '||SQLERRM);
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
     v_cant_error NUMBER;
     v_found BOOLEAN := FALSE;
     v_sih_cliente_antecedente_row ingres.sih_cliente_antecedente%ROWTYPE;
   BEGIN
     BEGIN
       --DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS');
       DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS');
       dbms_output.put_line('TERMINADO RUN JOB LIST_FILES_EQUIFAX_CONFIRMADOS');
     EXCEPTION
       WHEN OTHERS THEN
         IF SQLCODE = -27369 THEN
            dbms_output.put_line('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS. Sin archivos para listar. '||SQLERRM);
            RETURN;
         ELSE
             pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                                SQLERRM, 'Error al ejecutar Job LIST_FILES_EQUIFAX_CONFIRMADOS',
                                                'pr_apim_cartera_confirmados', NULL);
             bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                  p_texto => 'Error al ejecutar Job LIST_FILES_EQUIFAX_CONFIRMADOS, en bfapim.pkg_apim_carteras_equifax.pr_apim_cartera_confirmados. '||SQLERRM,
                                  p_codigo_mail => 'MTEI');
             dbms_output.put_line('Error en JOB. Procedimiento ABORTADO. '||SQLERRM);
             RETURN;
         END IF;
         --raise_application_error(-20120, 'Error en JOB LIST_FILES_EQUIFAX_CONFIRMADOS. '||SQLERRM);
     END;

     -- Cargar coleccion a partir de reporteRechazos.csv
     pr_apim_list_rep_rechazados;

     -- Cargar coleccion a partir de RECHAZADO.csv
     pr_apim_list_rechazado;

     -- Abrir el archivo de salida (donde se encuentra la lista de archivos obtenidos)
     archivo_entrada := utl_file.fopen(v_path_confirmados, 'output_mt.txt', 'R');
     DBMS_OUTPUT.ENABLE(1000000); -- Establece el tamaño del búfer en 1 MB
     LOOP

        BEGIN
          utl_file.get_line(archivo_entrada, linea);
          v_file_name := linea; --para tener ordenado el codigo se guarda en la variable

          IF v_file_name LIKE '%PROCESADO%' THEN
            v_esAlta := FALSE;
            IF fc_apim_existe_archivos_equifax(v_file_name) = FALSE THEN
                pr_apim_obt_fecha_esalta(v_file_name, v_fecha_nombre_archivo, v_esAlta);

                v_cant_error := 0;
                v_rowcount := 0;
            dbms_output.put_line('Procesando: '|| v_file_name);
                /*Tratamiento del o de los archivos csv*/
                file_handle := UTL_FILE.FOPEN(v_path_confirmados, v_file_name, 'R');
                LOOP
                  BEGIN
                      -- Leer una línea del archivo
                      UTL_FILE.GET_LINE(file_handle, file_line);
                      v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 1);

                      IF v_nro_doc != 'Documento' THEN
                        v_dioError := FALSE;
                        -- Separar los valores del CSV (usualmente separados por , o ;)
                        v_tipo_doc_str := pr_apim_2array2value (file_line, 2);
                        --DBMS_OUTPUT.PUT_LINE( 'Documento a procesar:' || v_nro_doc||' - v_tipo_doc_str: '|| v_tipo_doc_str);

                        pr_apim_obt_equivalencia_tipo_doc(v_tipo_doc_str, v_tipo_doc_num, v_dioError);

                        IF v_esAlta = TRUE AND nvl(v_dioError, FALSE) = FALSE THEN
                          -- OBTENER CODIGO DE CLIENTE Y OTROS DATOS DEL CLIENTE
                          pr_apim_obt_cliente_antecedente(v_nro_doc, v_tipo_doc_num, to_date(v_fecha_nombre_archivo, 'DDMMYYYY'),
                                                            v_sih_cliente_antecedente_row, v_dioError);

                          v_cliente_controlado := 'SI';
                          v_fecha_inicio := SYSDATE;

                          -- SE INSERTA EN SIH_CLIENTES_CONTROLADOS / se contabiliza los que fallaron
                          IF nvl(v_dioError, FALSE) = FALSE THEN
                              --pr_apim_insert_clientes_controlados(v_nro_doc, v_nombre1, v_nombre2, v_apellido, v_apellido2, v_apellido_casada, v_tipo_control, v_fecha_inicio, v_fecha_final, v_cliente_controlado, v_cod_cliente);
                              pr_apim_insert_clientes_controlados(v_nro_doc, v_sih_cliente_antecedente_row.primer_nombre,
                                                        v_sih_cliente_antecedente_row.segundo_nombre, v_sih_cliente_antecedente_row.primer_apellido,
                                                        v_sih_cliente_antecedente_row.segundo_apellido, v_sih_cliente_antecedente_row.apellido_casada,
                                                        v_sih_cliente_antecedente_row.tipo_control,   v_fecha_inicio,
                                                        v_sih_cliente_antecedente_row.fecha_final,    v_cliente_controlado,
                                                        v_sih_cliente_antecedente_row.cod_cliente,    v_dioError);
                          ELSE
                            v_cant_error := v_cant_error + 1;
                          END IF;

                          -- ACTUALIZAR CLIENTE_ANTECEDENTE
                          IF nvl(v_dioError, FALSE) = FALSE THEN

                              pr_apim_update_cliente_antecedente(P_NRO_DOC => v_nro_doc,
                                                                P_TIPO_DOC_NUM => v_tipo_doc_num,
                                                                p_fec_actualizacion => TRUNC(to_date(v_fecha_nombre_archivo, 'DDMMYYYY')),
                                                                P_COD_ESTADO_UPD => 'PR',
                                                                P_COMENTARIO_UPD => NULL,
                                                                P_FEC_UPD => SYSDATE,
                                                                P_DIOERROR => v_dioError);
                              v_rowcount := v_rowcount + SQL%ROWCOUNT;
                          END IF;


                        ELSE -- Aqui entraria cuando se trata de un PROCESADO BAJAS

                          IF nvl(v_dioError, FALSE) = FALSE THEN

                            -- DELETE DE sih_clientes_controlados
                            v_found := FALSE;
                            FOR i IN 1 .. my_nested_table.COUNT LOOP
                                IF my_nested_table(i) = v_nro_doc THEN
                                --dbms_output.put_line('Array encontrado: '|| v_nro_doc);
                                    v_found := TRUE;
                                    EXIT; -- Salir del loop si se encuentra el valor
                                END IF;
                            END LOOP;

                            IF NOT v_found THEN /*solo si no se encuentra en la coleccion se borra(si no se encuentra en reporteRechazos y RECHAZADOS)*/
                                pr_apim_elimina_clientes_controlados(v_nro_doc, NULL, v_dioError);
                                IF nvl(v_dioError, FALSE) = TRUE THEN
                                  v_cant_error := v_cant_error + 1;
                                END IF;
                            END IF;
                            --no se puede meter en el pr_apim_elimina_clientes_controlados, porque ese proc se llama desde varios
                            IF nvl(v_dioError, FALSE) = FALSE THEN
                                pr_apim_update_cliente_antecedente(P_NRO_DOC => v_nro_doc,
                                                            P_TIPO_DOC_NUM => v_tipo_doc_num,
                                                            p_fec_actualizacion => TRUNC(to_date(v_fecha_nombre_archivo, 'DDMMYYYY')),
                                                            P_COD_ESTADO_UPD => 'PR',
                                                            P_COMENTARIO_UPD => NULL,
                                                            P_FEC_UPD => NULL,
                                                            P_DIOERROR => v_dioError);

                                v_rowcount := v_rowcount + SQL%ROWCOUNT;
                                IF nvl(v_dioError, FALSE) = TRUE THEN
                                  v_cant_error := v_cant_error + 1;
                                END IF;
                            END IF;
                          END IF;
                        END IF;
                      END IF;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                          -- Finalizar el bucle cuando no haya más datos en el archivo
                          EXIT;
                  END;
                END LOOP;
                UTL_FILE.FCLOSE(file_handle); --cierra el csv de confirmados

                IF v_rowcount > 0 THEN
                  pr_apim_insert_apim_archivos_equifax(v_file_name, SYSDATE, 'PROCESADO', CASE
                                                                                          WHEN v_esAlta = TRUE THEN 'ALTA'
                                                                                          ELSE 'BAJA'
                                                                                      END);
                  COMMIT;
                  -- Correr script bash que mueve el archivo procesado
                  DBMS_OUTPUT.PUT_LINE( 'Moviendo documento '|| v_file_name);
                  pr_mover_archivo('/archivos_aplicacion/entrada/otras_instituciones/equifax/confirmados/'||v_file_name, '/archivos_aplicacion/entrada/otras_instituciones/equifax/procesados/');
                  DBMS_OUTPUT.PUT_LINE( 'Documento movido a: ' || '/archivos_aplicacion/entrada/otras_instituciones/equifax/procesados/');
                END IF;
                IF v_cant_error > 0 THEN
                  bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                      p_texto => v_cant_error||' Documentos no procesados, revise el log equifax. Archivo: '||v_file_name,
                                      p_codigo_mail => 'MTEI');
                END IF;
            END IF; -- fc_apim_existe_archivos_equifax
          ELSIF v_file_name LIKE '%RECHAZADO%' THEN
            IF fc_apim_existe_archivos_equifax(v_file_name) = FALSE THEN
               IF INSTR(v_file_name, '_Bajas_') > 0 THEN
                  v_fecha_nombre_archivo := SUBSTR(v_file_name, instr(v_file_name, '_Bajas_') + 7, 8);
               END IF;

               file_handle := UTL_FILE.FOPEN(v_path_confirmados, v_file_name, 'R');
               LOOP
                  BEGIN
                      -- Leer una línea del archivo
                      UTL_FILE.GET_LINE(file_handle, file_line);
                      v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 1);

                      IF v_nro_doc != 'Documento' THEN
                        v_dioError := FALSE;
                        -- Separar los valores del CSV (usualmente separados por , o ;)
                        v_tipo_doc_str := pr_apim_2array2value (file_line, 2);

                        -- OBTENER EQUIVALENCIA DEL TIPO DE DOCUMENTO
                        pr_apim_obt_equivalencia_tipo_doc(P_TIPO_DOC_STR => v_tipo_doc_str,
                                                          P_EQUIVALENCIA => v_tipo_doc_num,
                                                          P_DIOERROR => v_dioError);

                        -- ACTUALIZAR CLIENTE_ANTECEDENTE
                        IF nvl(v_dioError, FALSE) = FALSE THEN
                            pr_apim_update_cliente_antecedente(P_NRO_DOC => v_nro_doc,
                                                            P_TIPO_DOC_NUM => v_tipo_doc_num,
                                                            p_fec_actualizacion => TRUNC(to_date(v_fecha_nombre_archivo, 'DDMMYYYY')),
                                                            P_COD_ESTADO_UPD => 'PR',
                                                            P_COMENTARIO_UPD => 'Cliente no se puede dar de baja antes de 90 dias',
                                                            P_FEC_UPD => SYSDATE,
                                                            P_DIOERROR => v_dioError);
                          v_rowcount := v_rowcount + SQL%ROWCOUNT;
                        END IF;
                        IF nvl(v_dioError, FALSE) = TRUE THEN
                          v_cant_error := v_cant_error + 1;
                        END IF;
                      END IF;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                          -- Finalizar el bucle cuando no haya más datos en el archivo
                          EXIT;
                  END;
               END LOOP;
                UTL_FILE.FCLOSE(file_handle); --cierra el csv RECHAZADO

                IF v_rowcount > 0 THEN
                  pr_apim_insert_apim_archivos_equifax(v_file_name, SYSDATE, 'PROCESADO', CASE
                                                                                          WHEN v_esAlta = TRUE THEN 'ALTA'
                                                                                          ELSE 'BAJA'
                                                                                      END);
                  COMMIT;
                  -- Correr script bash que mueve el archivo procesado
                  DBMS_OUTPUT.PUT_LINE( 'Moviendo documento '|| v_file_name);
                  pr_mover_archivo('/archivos_aplicacion/entrada/otras_instituciones/equifax/confirmados/'||v_file_name, '/archivos_aplicacion/entrada/otras_instituciones/equifax/procesados/');
                  DBMS_OUTPUT.PUT_LINE( 'Documento movido a: ' || '/archivos_aplicacion/entrada/otras_instituciones/equifax/procesados/');
                END IF;
                IF v_cant_error > 0 THEN
                  bfapim.pr_apim_mail_equifax(p_asunto => 'Error proceso equifax',
                                      p_texto => v_cant_error||' Documentos no procesados, revise el log equifax. Archivo: '||v_file_name,
                                      p_codigo_mail => 'MTEI');
                END IF;



            END IF; -- Fin existe_archivo_equifax
          END IF; -- Fin ELSIF v_file_name LIKE '%RECHAZADO%'
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya más datos en el archivo
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
                                               cod_cliente IN VARCHAR2, P_DIOERROR OUT BOOLEAN)
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
        P_DIOERROR := TRUE;
        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                        SQLERRM, 'Error al insertar en sih_cliente_controlados',
                        'pkg_apim_carteras_equifax', NULL);
        DBMS_OUTPUT.PUT_LINE('Error al insertar en sih_cliente_controlados. '||SQLERRM);
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

   FUNCTION fc_apim_existe_archivos_equifax(p_nombre_archivo VARCHAR2)
   RETURN BOOLEAN
   IS
     v_existe VARCHAR2(1) := 'N';
   BEGIN
     BEGIN
       SELECT 'S' INTO v_existe
       FROM bfapim.APIM_ARCHIVOS_EQUIFAX
       WHERE nombre = p_nombre_archivo
       AND estado = 'PROCESADO';
     EXCEPTION
       WHEN OTHERS THEN
         v_existe := 'N';
     END;

     IF v_existe = 'S' THEN
       RETURN TRUE;
     ELSE
       RETURN FALSE;
     END IF;
   END;

   PROCEDURE pr_apim_elimina_clientes_controlados(P_DOCUMENTO IN VARCHAR2, P_COD_CLIENTE IN VARCHAR2 DEFAULT NULL, P_DIOERROR OUT BOOLEAN)
   AS
   BEGIN
     IF p_cod_cliente IS NOT NULL THEN
       DELETE FROM ingres.sih_clientes_controlados
       WHERE nro_doc_id = P_DOCUMENTO
         AND cod_cliente = P_COD_CLIENTE;
     ELSE
       DELETE FROM ingres.sih_clientes_controlados
       WHERE nro_doc_id = P_DOCUMENTO;
     END IF;
   EXCEPTION
      WHEN OTHERS THEN
        P_DIOERROR := TRUE;
        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                                                    SQLERRM, 'Error al borrar sih_cliente_controlados',
                                                    'pr_apim_elimina_clientes_controlados', NULL);
        dbms_output.put_line('Error al eliminar de ingres.sih_clientes_controlados. '||SQLERRM);
   END;

   /* ESTE PROCEDIMIENTO SE UTILIZA PARA GUARDAR EN UNA COLECCION LOS
      DOCUMENTOS EXISTENTES EN EL ARCHIVO ReporteRechazos(provehido por equifax) */
   PROCEDURE pr_apim_list_rep_rechazados
   AS
     v_counter NUMBER := 1;
     file_handle UTL_FILE.FILE_TYPE;
     file_line       VARCHAR2(32767);
     archivo_entrada UTL_FILE.FILE_TYPE;
     linea           varchar2(32767);
     v_nro_doc    VARCHAR2(100);
     v_file_name     VARCHAR2(255);
   BEGIN
     my_nested_table := nested_table_type();
     my_nested_table.DELETE;

      BEGIN
        --DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS');
        DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS');
        dbms_output.put_line('TERMINADO RUN JOB para listar archivos csv en PR_APIM_LIST_REP_RECHAZADOS');
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -27369 THEN
            dbms_output.put_line('BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS. Sin archivos para listar. '||SQLERRM);
            RETURN;
          ELSE
            dbms_output.put_line('Error o no existen *.csv en JOB BFAPIM.LIST_FILES_EQUIFAX_RECHAZOS. '||SQLERRM);
            RETURN;
          END IF;
      END;


      -- Abrir el archivo de salida (donde se encuentra la lista de archivos obtenidos)
      archivo_entrada := utl_file.fopen(v_scripts_directory_rechazados, 'output_mt.txt', 'R');
      LOOP

        BEGIN
          utl_file.get_line(archivo_entrada, linea);
          --AQUI PROCESAR EL ARCHIVO TXT OBTENIDO (se obtiene un archivo ej: listado.txt)
          v_file_name := linea; --para tener ordenado el codigo se guarda en la variable

          IF v_file_name LIKE '%Rechazos%' THEN

            IF fc_apim_existe_archivos_equifax(v_file_name) = FALSE THEN

                /*Tratamiento del o de los archivos csv de rechazados*/
                file_handle := UTL_FILE.FOPEN(v_scripts_directory_rechazados, v_file_name, 'R');

                LOOP
                  BEGIN
                      -- Leer una línea del archivo
                      UTL_FILE.GET_LINE(file_handle, file_line);
                      v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 2);

                      IF v_nro_doc != 'Documento' THEN
                        my_nested_table.EXTEND;

                        -- Asignar valores a la colección
                        my_nested_table(v_counter) := v_nro_doc;

                        -- Incrementar el contador
                        v_counter := v_counter + 1;

                      END IF;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                          -- Finalizar el bucle cuando no haya más datos en el archivo
                          EXIT;
                  END;
                END LOOP;
                UTL_FILE.FCLOSE(file_handle); --cierra el csv de Reprechazados
                dbms_output.put_line((v_counter - 1) || ' Documentos insertados en coleccion');
            END IF; -- fc_apim_existe_archivos_equifax
          END IF;
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya más datos en el archivo
                  EXIT;
        END;
      END LOOP;
      UTL_FILE.FCLOSE(archivo_entrada);
   END;

   PROCEDURE pr_apim_list_rechazado
   AS
     v_counter NUMBER;
     file_handle UTL_FILE.FILE_TYPE;
     file_line       VARCHAR2(32767);
     archivo_entrada UTL_FILE.FILE_TYPE;
     linea           varchar2(32767);
     v_nro_doc    VARCHAR2(100);
     v_file_name     VARCHAR2(255);
     v_path_confirmados varchar2(200) := 'ARCHIVOS_EXTERNOS#OTRAS_INSTITUCIONES$EQUIFAX$CONFIRMADOS';
     --v_indice_col PLS_INTEGER;
   BEGIN

     BEGIN
       v_counter := my_nested_table.count + 1;
     EXCEPTION
       WHEN OTHERS THEN
         IF SQLCODE = -6531 THEN
           my_nested_table := nested_table_type();
           my_nested_table.delete;
           v_counter := 1;
         END IF;
     END;


     BEGIN
       --DBMS_SCHEDULER.ENABLE('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS');
       DBMS_SCHEDULER.RUN_JOB('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS');
       dbms_output.put_line('TERMINADO RUN JOB LIST_FILES_EQUIFAX_CONFIRMADOS en pr_apim_list_rechazado');
     EXCEPTION
       WHEN OTHERS THEN
         IF SQLCODE = -27369 THEN
            dbms_output.put_line('BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS. Sin archivos para listar. '||SQLERRM);
            RETURN;
         ELSE
           dbms_output.put_line('Error o no existe/n *.csv en JOB BFAPIM.LIST_FILES_EQUIFAX_CONFIRMADOS. '||SQLERRM);
           RETURN;
         END IF;
     END;

     archivo_entrada := utl_file.fopen(v_path_confirmados, 'output_mt.txt', 'R');
      LOOP

        BEGIN
          utl_file.get_line(archivo_entrada, linea);
          --AQUI PROCESAR EL ARCHIVO TXT OBTENIDO (se obtiene un archivo ej: listado.txt)
          v_file_name := linea; --para tener ordenado el codigo se guarda en la variable
          IF v_file_name LIKE '%RECHAZADO%' THEN
            IF fc_apim_existe_archivos_equifax(v_file_name) = FALSE THEN

                /*Tratamiento del o de los archivos csv de rechazados*/
                file_handle := UTL_FILE.FOPEN(v_path_confirmados, v_file_name, 'R');

                LOOP
                  BEGIN
                      -- Leer una línea del archivo
                      UTL_FILE.GET_LINE(file_handle, file_line);
                      v_nro_doc := REGEXP_SUBSTR(file_line, '[^;]+', 1, 1);

                      IF v_nro_doc != 'Documento' THEN
                        my_nested_table.EXTEND;

                        -- Asignar valores a la colección
                        my_nested_table(v_counter) := v_nro_doc;

                        -- Incrementar el contador
                        v_counter := v_counter + 1;
                      END IF;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                          -- Finalizar el bucle cuando no haya más datos en el archivo
                          EXIT;
                  END;
                END LOOP;
                UTL_FILE.FCLOSE(file_handle); --cierra el csv de rechazados
                dbms_output.put_line((v_counter - 1) || ' Documentos total insertados en coleccion');
            END IF; -- fc_apim_existe_archivos_equifax
          END IF;
        EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  -- Finalizar el bucle cuando no haya más datos en el archivo
                  EXIT;
        END;
      END LOOP;
      UTL_FILE.FCLOSE(archivo_entrada);

   END;

   PROCEDURE pr_apim_obt_cliente_antecedente(P_NRO_DOC IN VARCHAR2, P_TIPO_DOC_NUM IN NUMBER, P_FECHA IN DATE,
                                             P_ROW_CLIENTE_ANT OUT ingres.sih_cliente_antecedente%ROWTYPE,
                                             P_DIOERROR OUT BOOLEAN)
   AS
     v_cod_cliente ingres.sih_cliente_antecedente.cod_cliente%TYPE;
     v_nombre ingres.sih_cliente_antecedente.primer_nombre%TYPE;
     v_apellido ingres.sih_cliente_antecedente.primer_apellido%TYPE;
     v_tipo_control ingres.sih_cliente_antecedente.tipo_control%TYPE;
     v_tipo_movimiento ingres.sih_cliente_antecedente.tipo_movimiento%TYPE;
     v_cod_estado ingres.sih_cliente_antecedente.cod_estado%TYPE;

     v_sih_cliente_antecedente_row ingres.sih_cliente_antecedente%ROWTYPE;
   BEGIN
      SELECT *
      INTO v_sih_cliente_antecedente_row
      FROM ingres.sih_cliente_antecedente
      WHERE nro_doc_id = P_NRO_DOC
        AND cod_tipo_doc_id = P_TIPO_DOC_NUM
        AND trunc(fec_actualizacion) = P_FECHA
        AND ROWNUM = 1;
        P_ROW_CLIENTE_ANT := v_sih_cliente_antecedente_row;
   EXCEPTION
      WHEN OTHERS THEN
      P_DIOERROR := TRUE;
      pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                          SQLERRM, 'Error al recuperar datos de sih_cliente_antecedente, documento: '||P_NRO_DOC||' Archivo: '/*|| v_file_name*/,
                          'pr_apim_cartera_confirmados', NULL);
      DBMS_OUTPUT.PUT_LINE('Error al recuperar en sih_cliente_antecedente. '||P_NRO_DOC||' '||SQLERRM);
   END;

   PROCEDURE pr_apim_obt_equivalencia_tipo_doc(P_TIPO_DOC_STR IN VARCHAR2, P_EQUIVALENCIA OUT NUMBER, P_DIOERROR OUT BOOLEAN)
   AS
     v_tipo_doc_num ingres.tipo_documento_id.cod_tipo_doc_id%TYPE;
   BEGIN
     SELECT cod_tipo_doc_id INTO v_tipo_doc_num
      from ingres.tipo_documento_id
      WHERE TRIM(abrev_tipo_doc_id) = TRIM(P_TIPO_DOC_STR);
     P_EQUIVALENCIA := v_tipo_doc_num;
   EXCEPTION
      WHEN OTHERS THEN
        P_DIOERROR := TRUE;
        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                            SQLERRM, 'Error al consultar ingres.tipo_documento_id, '|| SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2),
                            'pr_apim_cartera_rechazados', NULL);
        DBMS_OUTPUT.PUT_LINE('Error al recuperar tipo documento. '||SQLERRM);
   END;

   FUNCTION fn_apim_existe_cliente_verificar(P_COD_CLIENTE VARCHAR2) RETURN NUMBER
   AS
     v_existe_cv NUMBER := 0;
   BEGIN
      BEGIN
        SELECT 1 INTO v_existe_cv
        FROM ingres.sih_cliente_verificar
        WHERE cod_cliente = P_COD_CLIENTE;
      EXCEPTION
        WHEN no_data_found THEN
          v_existe_cv := 0;
        WHEN too_many_rows THEN
          v_existe_cv := 1;
      END;

      RETURN v_existe_cv;
   END;

   PROCEDURE pr_apim_update_cliente_verificar(P_COD_CLIENTE IN VARCHAR2, P_FEC_ACTUALIZACION IN DATE, P_COMENTARIO IN VARCHAR2,
                                              P_INSERTADO OUT BOOLEAN, P_DIOERROR OUT BOOLEAN)
   AS

   BEGIN
     UPDATE ingres.sih_cliente_verificar
      SET fec_actualizacion = P_FEC_ACTUALIZACION,
          comentario = P_COMENTARIO
      WHERE cod_cliente = P_COD_CLIENTE;

      IF SQL%ROWCOUNT > 0 THEN
        P_INSERTADO := TRUE;
      ELSE
        P_INSERTADO := FALSE;
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
        P_DIOERROR := TRUE;
        P_INSERTADO := FALSE;
        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                              SQLERRM,'Error al actualizar datos en sih_cliente_verificar. ' || SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2),
                              'pr_apim_cartera_rechazados', NULL);
        dbms_output.put_line('Error al actualizar cliente_verificar. '||SQLERRM);
   END;

   PROCEDURE pr_apim_update_cliente_antecedente(P_NRO_DOC IN VARCHAR2, P_TIPO_DOC_NUM IN NUMBER, P_FEC_ACTUALIZACION IN DATE,
                                                P_COD_ESTADO_UPD IN VARCHAR2, P_COMENTARIO_UPD IN VARCHAR2, P_FEC_UPD IN DATE,
                                                P_DIOERROR OUT BOOLEAN)
   AS

   BEGIN
     IF P_COD_ESTADO_UPD IS NOT NULL AND P_COMENTARIO_UPD IS NOT NULL AND P_FEC_UPD IS NOT NULL THEN
       UPDATE ingres.sih_cliente_antecedente
          SET cod_estado = P_COD_ESTADO_UPD,
              comentario = P_COMENTARIO_UPD,
              fec_actualizacion = P_FEC_UPD
          WHERE nro_doc_id = P_NRO_DOC
            AND cod_tipo_doc_id = P_TIPO_DOC_NUM
            AND TRUNC(fec_actualizacion) = P_FEC_ACTUALIZACION;

     ELSIF P_COD_ESTADO_UPD IS NOT NULL AND P_COMENTARIO_UPD IS NOT NULL AND P_FEC_UPD IS NULL THEN
       UPDATE ingres.sih_cliente_antecedente
          SET cod_estado = P_COD_ESTADO_UPD,
              comentario = P_COMENTARIO_UPD
          WHERE nro_doc_id = P_NRO_DOC
            AND cod_tipo_doc_id = P_TIPO_DOC_NUM
            AND TRUNC(fec_actualizacion) = P_FEC_ACTUALIZACION;

     ELSIF P_COD_ESTADO_UPD IS NOT NULL AND P_COMENTARIO_UPD IS NULL AND P_FEC_UPD IS NULL THEN
       UPDATE ingres.sih_cliente_antecedente
          SET cod_estado = P_COD_ESTADO_UPD
          WHERE nro_doc_id = P_NRO_DOC
            AND cod_tipo_doc_id = P_TIPO_DOC_NUM
            AND TRUNC(fec_actualizacion) = P_FEC_ACTUALIZACION;

     ELSIF P_COD_ESTADO_UPD IS NOT NULL AND P_COMENTARIO_UPD IS NULL AND P_FEC_UPD IS NOT NULL THEN
       UPDATE ingres.sih_cliente_antecedente
          SET cod_estado = P_COD_ESTADO_UPD,
             fec_actualizacion = P_FEC_UPD
          WHERE nro_doc_id = P_NRO_DOC
            AND cod_tipo_doc_id = P_TIPO_DOC_NUM
            AND TRUNC(fec_actualizacion) = P_FEC_ACTUALIZACION;

     END IF;
    EXCEPTION
      WHEN OTHERS THEN
        P_DIOERROR := TRUE;
        pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                          SQLERRM, 'Error al actualizar ingres.sih_cliente_antecedente',
                          'pr_apim_cartera_confirmados. Documento: '||P_NRO_DOC, NULL);
        DBMS_OUTPUT.PUT_LINE('Error al actualizar ingres.sih_cliente_antecedente. '||SQLERRM);
   END;

   PROCEDURE pr_apim_obt_comment_tipo_doc(P_LINEA IN VARCHAR2, P_COMMENT OUT VARCHAR2, P_TIPO_DOC_STR OUT VARCHAR2, P_DIOERROR OUT BOOLEAN)
   AS
   BEGIN
     P_COMMENT := REPLACE(REPLACE(TRIM(pr_apim_2array2value (P_LINEA, 18)), CHR(13), ''), CHR(10), ''); --pr_apim_2array2value (file_line, 18);
     P_TIPO_DOC_STR := pr_apim_2array2value (P_LINEA, 3);
   EXCEPTION
    WHEN OTHERS THEN
      P_DIOERROR := TRUE;
      pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                          SQLERRM, 'Error al extraer datos de una linea del archivo csv. ' || SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2),
                          'pr_apim_cartera_rechazados', NULL);
      DBMS_OUTPUT.PUT_LINE('Error al obtener datos de la linea. '||SQLERRM);
   END;


   PROCEDURE pr_apim_update_ca(P_NRO_DOC IN VARCHAR2, P_COD_TIPO_DOC_NUM IN NUMBER, P_COMENTARIO IN VARCHAR2,
                               P_DIOERROR OUT BOOLEAN, P_ROWCOUNT OUT NUMBER)
   AS
     v_max_date DATE;
     v_dioerror_local BOOLEAN := FALSE;
     v_rowcount_local NUMBER := 0;
   BEGIN

       BEGIN
        SELECT MAX(fec_actualizacion)
        INTO v_max_date
        FROM ingres.sih_cliente_antecedente
        WHERE nro_doc_id = P_NRO_DOC
          AND cod_tipo_doc_id = P_COD_TIPO_DOC_NUM;

       EXCEPTION
        WHEN OTHERS THEN
          v_dioerror_local := TRUE;
          pr_apim_insert_apim_equifax_log(SYSDATE, SQLCODE,
                              SQLERRM, 'Error al actualizar sih_cliente_antecedente. '|| SUBSTR(SQLERRM, INSTR(SQLERRM, ': ') + 2),
                              'pr_apim_cartera_rechazados', NULL);
          dbms_output.put_line('Error al actualizar en ingres.sih_cliente_antecedente. '||SQLERRM);
       END;

        IF v_dioerror_local = FALSE THEN
            pr_apim_update_cliente_antecedente(P_NRO_DOC => P_NRO_DOC,
                                              P_TIPO_DOC_NUM => P_COD_TIPO_DOC_NUM,
                                              p_fec_actualizacion => trunc(v_max_date),
                                              P_COD_ESTADO_UPD => 'ER',
                                              P_COMENTARIO_UPD => P_COMENTARIO,
                                              P_FEC_UPD => SYSDATE,
                                              P_DIOERROR => P_DIOERROR);
            v_rowcount_local := v_rowcount_local + SQL%ROWCOUNT;
         ELSE
           P_DIOERROR := TRUE;
           RETURN;
         END IF;

         P_ROWCOUNT := v_rowcount_local;

   END;

   PROCEDURE pr_apim_obt_fecha_esalta(P_FILENAME IN VARCHAR2, P_FECHA_NOMARCHIVO OUT VARCHAR2, P_ESALTA OUT BOOLEAN)
   AS
   BEGIN
      IF instr(P_FILENAME, '_Altas_') > 0 THEN
        P_FECHA_NOMARCHIVO := SUBSTR(P_FILENAME, instr(P_FILENAME, '_Altas_') + 7, 8);
        P_ESALTA := TRUE;
      ELSE
        P_FECHA_NOMARCHIVO := SUBSTR(P_FILENAME, instr(P_FILENAME, '_Bajas_') + 7, 8);
        P_ESALTA := FALSE;
      END IF;
   END;
END;
/
