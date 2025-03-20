import { MarketplaceMeteringClient, ResolveCustomerCommand } from "@aws-sdk/client-marketplace-metering";

const tempCredentials = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  sessionToken: process.env.AWS_SESSION_TOKEN
};

const marketplacemetering = new MarketplaceMeteringClient({
  region: 'us-west-2',
  credentials: tempCredentials
});

const resolveCustomerParams = {
  RegistrationToken: 'asdfa',
};

const resolve = new ResolveCustomerCommand(resolveCustomerParams);

void async function () {
  try {
    const data = await marketplacemetering.send(resolve);
    console.log(data);
  } catch (error) {
    console.log(error)
  }
}();