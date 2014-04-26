#!/usr/bin/env python3

import asyncio
import functools
import logging
import sys

BUFFER_SIZE = 65536

def createLogger():
  logger = logging.getLogger('proxy')
  logger.setLevel(logging.INFO)

  consoleHandler = logging.StreamHandler()
  consoleHandler.setLevel(logging.DEBUG)

  formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  consoleHandler.setFormatter(formatter)

  logger.addHandler(consoleHandler)

  return logger

logger = createLogger()

def client_connection_string(writer):
  return '{} -> {}'.format(
    writer.get_extra_info('peername'),
    writer.get_extra_info('sockname'))

def remote_connection_string(writer):
  return '{} -> {}'.format(
    writer.get_extra_info('sockname'),
    writer.get_extra_info('peername'))

@asyncio.coroutine
def proxy_data_task(reader, writer, connection_string):
  try:
    while True:
      buffer = yield from reader.read(BUFFER_SIZE)
      if not buffer:
        break
      writer.write(buffer)
      yield from writer.drain()
  except Exception as e:
    logger.info('proxy_data_task exception {}'.format(e))
  finally:
    writer.close()
    logger.info('close connection {}'.format(connection_string))

@asyncio.coroutine
def accept_client_task(client_reader, client_writer, remote_address, remote_port):
  client_string = client_connection_string(client_writer)
  logger.info('accept connection {}'.format(client_string))
  try:
    (remote_reader, remote_writer) = yield from asyncio.wait_for(
      asyncio.open_connection(host = remote_address, port = remote_port),
      timeout = 1)
  except asyncio.TimeoutError:
    logger.info('connect timeout')
    logger.info('close connection {}'.format(client_string))
    client_writer.close()
  except Exception as e:
    logger.info('error connecting to remote server: {}'.format(e))
    logger.info('close connection {}'.format(client_string))
    client_writer.close()
  else:
    remote_string = remote_connection_string(remote_writer)
    logger.info('connect to remote {}'.format(remote_string))
    asyncio.async(proxy_data_task(client_reader, remote_writer, remote_string))
    asyncio.async(proxy_data_task(remote_reader, client_writer, client_string))

def parse_addr_port_string(addr_port_string):
  addr_port_list = addr_port_string.rsplit(':', 1)
  return (addr_port_list[0], int(addr_port_list[1]))

def print_usage():
  logger.error(
    'Usage: {} <listen addr> [<listen addr> ...] <remote addr>'.format(
      sys.argv[0]))

def main():
  if (len(sys.argv) < 3):
    print_usage()
    sys.exit(1)

  local_address_port_list = map(parse_addr_port_string, sys.argv[1:-1])
  (remote_address, remote_port) = parse_addr_port_string(sys.argv[-1])

  for (local_address, local_port) in local_address_port_list:
    asyncio.async(
      asyncio.start_server(
        functools.partial(
          accept_client_task, remote_address = remote_address, remote_port = remote_port),
        host = local_address, port = local_port))
    logger.info('listening on {}:{}'.format(local_address, local_port))
  try:
    loop = asyncio.get_event_loop()
    loop.run_forever()
  except KeyboardInterrupt:
    pass

if __name__ == '__main__':
  main()
